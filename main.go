package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	serverName    = "mycel-mcp"
	serverVersion = "0.1.0"

	defaultProtocolVersion = "2024-11-05"
	defaultMaxResults      = int32(25)
	hardMaxResults         = int32(200)
	defaultCallTimeout     = 30 * time.Second
)

type config struct {
	DefaultSpaceID  string
	DefaultDomainID string
	MaxResults      int32
	WriteEnabled    bool
	AdminEnabled    bool
}

func loadConfigFromEnv() config {
	maxResults := int32FromEnv("MYCEL_MCP_MAX_RESULTS", defaultMaxResults)
	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}
	if maxResults > hardMaxResults {
		maxResults = hardMaxResults
	}
	return config{
		DefaultSpaceID:  strings.TrimSpace(os.Getenv("MYCEL_MCP_DEFAULT_SPACE_ID")),
		DefaultDomainID: strings.TrimSpace(os.Getenv("MYCEL_MCP_DEFAULT_DOMAIN_ID")),
		MaxResults:      maxResults,
		WriteEnabled:    boolFromEnv("MYCEL_MCP_WRITE_ENABLED"),
		AdminEnabled:    boolFromEnv("MYCEL_MCP_ADMIN_ENABLED"),
	}
}

func int32FromEnv(name string, fallback int32) int32 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsed)
}

func boolFromEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}

type app struct {
	cfg      config
	mycelCfg mycel.Config

	mu     sync.Mutex
	client *mycel.Client
}

func newAppFromEnv() *app {
	mycelCfg := mycel.ConfigFromEnv()
	if strings.TrimSpace(mycelCfg.ClientName) == "" || mycelCfg.ClientName == "mycel-go-sdk" {
		mycelCfg.ClientName = serverName
	}
	if strings.TrimSpace(mycelCfg.Platform) == "" || mycelCfg.Platform == "go" {
		mycelCfg.Platform = "mcp"
	}
	if strings.TrimSpace(mycelCfg.DeviceLabel) == "" {
		mycelCfg.DeviceLabel = serverName
	}
	if mycelCfg.CallTimeout <= 0 {
		mycelCfg.CallTimeout = defaultCallTimeout
	}
	return &app{cfg: loadConfigFromEnv(), mycelCfg: mycelCfg}
}

func (a *app) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client == nil {
		return nil
	}
	err := a.client.Close()
	a.client = nil
	return err
}

func (a *app) mycelClient(ctx context.Context) (*mycel.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		return a.client, nil
	}
	client, err := mycel.Dial(ctx, a.mycelCfg)
	if err != nil {
		return nil, err
	}
	// A refresh-token-only configuration is useful for MCP clients, but the Go
	// SDK intentionally refreshes based on access-token expiry. Bootstrap an
	// access token explicitly when only a durable refresh token was supplied.
	if strings.TrimSpace(a.mycelCfg.AccessToken) == "" && strings.TrimSpace(a.mycelCfg.RefreshToken) != "" {
		if _, err := client.Refresh(ctx, a.mycelCfg.RefreshToken); err != nil {
			_ = client.Close()
			return nil, err
		}
	}
	a.client = client
	return a.client, nil
}

func (a *app) effectiveSpaceID(value string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return a.cfg.DefaultSpaceID
}

func (a *app) effectiveDomainID(value string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return a.cfg.DefaultDomainID
}

func (a *app) requireSpaceID(value string) (string, error) {
	spaceID := a.effectiveSpaceID(value)
	if spaceID == "" {
		return "", errors.New("space_id is required; pass space_id or set MYCEL_MCP_DEFAULT_SPACE_ID")
	}
	return spaceID, nil
}

func (a *app) requireDomainID(value string) (string, error) {
	domainID := a.effectiveDomainID(value)
	if domainID == "" {
		return "", errors.New("domain_id is required; pass domain_id or set MYCEL_MCP_DEFAULT_DOMAIN_ID")
	}
	return domainID, nil
}

func (a *app) requireScope(spaceValue, domainValue string) (string, string, error) {
	spaceID, err := a.requireSpaceID(spaceValue)
	if err != nil {
		return "", "", err
	}
	domainID, err := a.requireDomainID(domainValue)
	if err != nil {
		return "", "", err
	}
	return spaceID, domainID, nil
}

func (a *app) boundedPageSize(value int32) int32 {
	if value <= 0 {
		value = a.cfg.MaxResults
	}
	if value <= 0 {
		value = defaultMaxResults
	}
	if value > a.cfg.MaxResults && a.cfg.MaxResults > 0 {
		value = a.cfg.MaxResults
	}
	if value > hardMaxResults {
		value = hardMaxResults
	}
	return value
}

func (a *app) withReadTransaction(ctx context.Context, spaceID, domainID string, fn func(context.Context, *mycel.Client, string) (any, error)) (any, error) {
	spaceID, domainID, err := a.requireScope(spaceID, domainID)
	if err != nil {
		return nil, err
	}
	client, err := a.mycelClient(ctx)
	if err != nil {
		return nil, err
	}
	sessionID, err := client.OpenSession(ctx, spaceID, domainID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.CloseSession(context.Background(), sessionID) }()

	txID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.CloseTransaction(context.Background(), txID) }()

	return fn(ctx, client, txID)
}

type rpcRequest struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type server struct {
	app   *app
	tools map[string]tool
}

func newServer(a *app) *server {
	tools := newTools(a)
	return &server{app: a, tools: tools}
}

func (s *server) serve(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	encoder := json.NewEncoder(out)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			if err := encoder.Encode(rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: -32700, Message: "parse error", Data: err.Error()}}); err != nil {
				return err
			}
			continue
		}
		if req.ID == nil {
			if s.handleNotification(req) {
				return nil
			}
			continue
		}
		res := s.handleRequest(context.Background(), req)
		if err := encoder.Encode(res); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (s *server) handleNotification(req rpcRequest) bool {
	// MCP lifecycle notifications such as notifications/initialized and
	// notifications/cancelled do not require responses. Unknown notifications are
	// ignored per JSON-RPC practice. After shutdown, clients may send exit to ask
	// the server process to terminate.
	return req.Method == "exit"
}

func (s *server) handleRequest(ctx context.Context, req rpcRequest) rpcResponse {
	id := json.RawMessage("null")
	if req.ID != nil {
		id = *req.ID
	}
	result, errResp := s.dispatch(ctx, req)
	if errResp != nil {
		return rpcResponse{JSONRPC: "2.0", ID: id, Error: errResp}
	}
	return rpcResponse{JSONRPC: "2.0", ID: id, Result: result}
}

func (s *server) dispatch(ctx context.Context, req rpcRequest) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return s.initializeResult(req.Params), nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return s.toolsListResult(), nil
	case "tools/call":
		result, err := s.callTool(ctx, req.Params)
		if err != nil {
			return nil, err
		}
		return result, nil
	case "resources/list":
		return map[string]any{"resources": []any{}}, nil
	case "prompts/list":
		return map[string]any{"prompts": []any{}}, nil
	case "shutdown":
		return map[string]any{}, nil
	default:
		return nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
}

func (s *server) initializeResult(params json.RawMessage) map[string]any {
	protocolVersion := defaultProtocolVersion
	var initParams struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(params, &initParams)
	if strings.TrimSpace(initParams.ProtocolVersion) != "" {
		protocolVersion = initParams.ProtocolVersion
	}
	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    serverName,
			"version": serverVersion,
		},
		"instructions": "Read-only MycelDB MCP server. Use search, graph, metadata, schema, and GQL tools against the configured myceld daemon. Mutating operations are intentionally not exposed in this first version.",
	}
}

func (s *server) toolsListResult() map[string]any {
	names := make([]string, 0, len(s.tools))
	for name := range s.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	tools := make([]any, 0, len(names))
	for _, name := range names {
		tool := s.tools[name]
		tools = append(tools, map[string]any{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": tool.InputSchema,
		})
	}
	return map[string]any{"tools": tools}
}

type callToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func (s *server) callTool(ctx context.Context, params json.RawMessage) (any, *rpcError) {
	var call callToolParams
	if err := decodeJSON(params, &call); err != nil {
		return nil, &rpcError{Code: -32602, Message: "invalid tools/call params", Data: err.Error()}
	}
	tool, ok := s.tools[call.Name]
	if !ok {
		return nil, &rpcError{Code: -32602, Message: "unknown tool: " + call.Name}
	}
	value, err := tool.Handler(ctx, call.Arguments)
	if err != nil {
		return map[string]any{
			"content": []any{map[string]any{"type": "text", "text": err.Error()}},
			"isError": true,
		}, nil
	}
	text, err := resultText(value)
	if err != nil {
		return map[string]any{
			"content": []any{map[string]any{"type": "text", "text": err.Error()}},
			"isError": true,
		}, nil
	}
	return map[string]any{
		"content": []any{map[string]any{"type": "text", "text": text}},
	}, nil
}

type tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Handler     func(context.Context, json.RawMessage) (any, error)
}

func newTools(a *app) map[string]tool {
	tools := []tool{
		newMCPConfigTool(a),
		newWhoAmITool(a),
		newGetAccessTool(a),
		newListSpacesTool(a),
		newGetSpaceTool(a),
		newListDomainsTool(a),
		newGetDomainTool(a),
		newGetDomainSchemaTool(a),
		newSearchTool(a),
		newSemanticSearchTool(a),
		newListSemanticRulesTool(a),
		newExecuteGQLReadOnlyTool(a),
		newExplainGQLTool(a),
		newGetNodeTool(a),
		newListNodesTool(a),
		newListEdgesTool(a),
		newListChildrenTool(a),
		newGetParentTool(a),
		newListTagsTool(a),
		newListPropertyNamesTool(a),
		newGetLexicalIndexStatusTool(a),
	}
	out := make(map[string]tool, len(tools))
	for _, t := range tools {
		out[t.Name] = t
	}
	return out
}

func newMCPConfigTool(a *app) tool {
	return tool{
		Name:        "mycel_mcp_config",
		Description: "Show non-secret Mycel MCP configuration such as daemon address, defaults, result limits, and enabled modes.",
		InputSchema: emptySchema(),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			return map[string]any{
				"daemon_addr":       a.mycelCfg.Addr,
				"default_space_id":  a.cfg.DefaultSpaceID,
				"default_domain_id": a.cfg.DefaultDomainID,
				"max_results":       a.cfg.MaxResults,
				"write_enabled":     a.cfg.WriteEnabled,
				"admin_enabled":     a.cfg.AdminEnabled,
				"server_name":       serverName,
				"server_version":    serverVersion,
			}, nil
		},
	}
}

func newWhoAmITool(a *app) tool {
	return tool{
		Name:        "mycel_whoami",
		Description: "Return the Mycel principal authenticated by this MCP server.",
		InputSchema: emptySchema(),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Auth.WhoAmI(callCtx, &commonv1.WhoAmIRequest{})
		},
	}
}

func newGetAccessTool(a *app) tool {
	type args struct {
		Scope    string `json:"scope,omitempty"`
		SpaceID  string `json:"space_id,omitempty"`
		DomainID string `json:"domain_id,omitempty"`
	}
	return tool{
		Name:        "mycel_get_access",
		Description: "Return effective roles and capabilities for the authenticated Mycel principal, optionally scoped to system, space, or domain.",
		InputSchema: objectSchema(map[string]any{
			"scope":     enumStringSchema([]string{"system", "space", "domain"}, "Optional access scope filter."),
			"space_id":  stringSchema("Space ID for space/domain scope. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id": stringSchema("Domain ID for domain scope. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
		}, nil),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			scope, err := a.accessScope(in.Scope, in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Auth.GetMyAccess(callCtx, &commonv1.GetMyAccessRequest{Scope: scope})
		},
	}
}

func (a *app) accessScope(scopeValue, spaceValue, domainValue string) (*commonv1.AccessScope, error) {
	scopeValue = strings.ToLower(strings.TrimSpace(scopeValue))
	if scopeValue == "" && strings.TrimSpace(spaceValue) == "" && strings.TrimSpace(domainValue) == "" {
		return nil, nil
	}
	var typ commonv1.AccessScopeType
	scope := &commonv1.AccessScope{}
	switch scopeValue {
	case "", "system":
		typ = commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SYSTEM
	case "space":
		typ = commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SPACE
		spaceID, err := a.requireSpaceID(spaceValue)
		if err != nil {
			return nil, err
		}
		scope.SpaceId = &spaceID
	case "domain":
		typ = commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_DOMAIN
		spaceID, domainID, err := a.requireScope(spaceValue, domainValue)
		if err != nil {
			return nil, err
		}
		scope.SpaceId = &spaceID
		scope.DomainId = &domainID
	default:
		return nil, errors.New("scope must be one of: system, space, domain")
	}
	scope.Type = typ
	return scope, nil
}

func newListSpacesTool(a *app) tool {
	type args struct {
		PageSize        int32  `json:"page_size,omitempty"`
		PageToken       string `json:"page_token,omitempty"`
		IncludeArchived bool   `json:"include_archived,omitempty"`
	}
	return tool{
		Name:        "mycel_list_spaces",
		Description: "List Mycel spaces visible to the authenticated principal.",
		InputSchema: paginationSchema(map[string]any{
			"include_archived": boolSchema("Include archived spaces."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Space.ListSpaces(callCtx, &clientv1.ListSpacesRequest{PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, IncludeArchived: in.IncludeArchived})
		},
	}
}

func newGetSpaceTool(a *app) tool {
	type args struct {
		SpaceID string `json:"space_id,omitempty"`
	}
	return tool{
		Name:        "mycel_get_space",
		Description: "Get one visible Mycel space by ID. Uses MYCEL_MCP_DEFAULT_SPACE_ID when space_id is omitted.",
		InputSchema: objectSchema(map[string]any{"space_id": stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted.")}, nil),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			spaceID, err := a.requireSpaceID(in.SpaceID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Space.GetSpace(callCtx, &clientv1.GetSpaceRequest{SpaceId: spaceID})
		},
	}
}

func newListDomainsTool(a *app) tool {
	type args struct {
		SpaceID       string `json:"space_id,omitempty"`
		PageSize      int32  `json:"page_size,omitempty"`
		PageToken     string `json:"page_token,omitempty"`
		IncludeSystem bool   `json:"include_system,omitempty"`
	}
	return tool{
		Name:        "mycel_list_domains",
		Description: "List domains visible to the authenticated principal within a Mycel space.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":       stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"include_system": boolSchema("Include system/internal domains when authorized."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			spaceID, err := a.requireSpaceID(in.SpaceID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Domain.ListDomains(callCtx, &clientv1.ListDomainsRequest{SpaceId: spaceID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, IncludeSystem: in.IncludeSystem})
		},
	}
}

func newGetDomainTool(a *app) tool {
	type args struct {
		SpaceID  string `json:"space_id,omitempty"`
		DomainID string `json:"domain_id,omitempty"`
		Key      string `json:"key,omitempty"`
	}
	return tool{
		Name:        "mycel_get_domain",
		Description: "Get a visible Mycel domain by domain_id or key within a space. Defaults may be provided through MYCEL_MCP_DEFAULT_SPACE_ID and MYCEL_MCP_DEFAULT_DOMAIN_ID.",
		InputSchema: objectSchema(map[string]any{
			"space_id":  stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id": stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when key is omitted."),
			"key":       stringSchema("Stable domain key, for example default. Used instead of domain_id when supplied."),
		}, nil),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			spaceID, err := a.requireSpaceID(in.SpaceID)
			if err != nil {
				return nil, err
			}
			domainID := strings.TrimSpace(in.DomainID)
			key := strings.TrimSpace(in.Key)
			if domainID == "" && key == "" {
				domainID, err = a.requireDomainID("")
				if err != nil {
					return nil, errors.New("domain_id or key is required; pass one or set MYCEL_MCP_DEFAULT_DOMAIN_ID")
				}
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Domain.GetDomain(callCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, DomainId: domainID, Key: key})
		},
	}
}

func newGetDomainSchemaTool(a *app) tool {
	type args struct {
		DomainID string `json:"domain_id,omitempty"`
	}
	return tool{
		Name:        "mycel_get_domain_schema",
		Description: "Get the GWL schema for a Mycel domain.",
		InputSchema: objectSchema(map[string]any{"domain_id": stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted.")}, nil),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			domainID, err := a.requireDomainID(in.DomainID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Schema.GetDomainSchema(callCtx, &clientv1.GetDomainSchemaRequest{DomainId: domainID})
		},
	}
}

type searchPropertyFilterArg struct {
	Path     string   `json:"path"`
	Operator string   `json:"operator"`
	Values   []string `json:"values,omitempty"`
}

func newSearchTool(a *app) tool {
	type args struct {
		SpaceID             string                    `json:"space_id,omitempty"`
		DomainID            string                    `json:"domain_id,omitempty"`
		Query               string                    `json:"query"`
		Mode                string                    `json:"mode,omitempty"`
		PageSize            int32                     `json:"page_size,omitempty"`
		PageToken           string                    `json:"page_token,omitempty"`
		IncludeDiagnostics  bool                      `json:"include_diagnostics,omitempty"`
		AllowStale          bool                      `json:"allow_stale,omitempty"`
		CandidateCount      int32                     `json:"candidate_count,omitempty"`
		NodeLabels          []string                  `json:"node_labels,omitempty"`
		NodeIDs             []string                  `json:"node_ids,omitempty"`
		PropertyFilters     []searchPropertyFilterArg `json:"property_filters,omitempty"`
		SemanticRuleID      string                    `json:"semantic_rule_id,omitempty"`
		EmbeddingBindingKey string                    `json:"embedding_binding_key,omitempty"`
		MinScore            *float64                  `json:"min_score,omitempty"`
		LexicalWeight       float64                   `json:"lexical_weight,omitempty"`
		SemanticWeight      float64                   `json:"semantic_weight,omitempty"`
		RequireBoth         bool                      `json:"require_both,omitempty"`
	}
	return tool{
		Name:        "mycel_search",
		Description: "Run lexical or hybrid search over graph content in a Mycel space/domain. For direct semantic-only search, use mycel_semantic_search.",
		InputSchema: objectSchema(map[string]any{
			"space_id":              stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":             stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"query":                 stringSchema("Lexical query string, or shared lexical/semantic text for hybrid mode."),
			"mode":                  enumStringSchema([]string{"lexical", "hybrid"}, "Search mode. Defaults to lexical."),
			"page_size":             intSchema("Maximum results to return. Capped by MYCEL_MCP_MAX_RESULTS."),
			"page_token":            stringSchema("Opaque continuation token from a prior search."),
			"include_diagnostics":   boolSchema("Include safe query diagnostics when available."),
			"allow_stale":           boolSchema("Allow stale lexical index results."),
			"candidate_count":       intSchema("Candidate count for lexical/semantic retrieval before fusion."),
			"node_labels":           arrayStringSchema("All listed node labels must be present."),
			"node_ids":              arrayStringSchema("Restrict candidates to these node IDs."),
			"property_filters":      propertyFiltersSchema(),
			"semantic_rule_id":      stringSchema("Hybrid mode: semantic rule ID to search."),
			"embedding_binding_key": stringSchema("Hybrid mode: embedding binding key. Requires semantic_rule_id when set."),
			"min_score":             numberSchema("Hybrid mode: optional semantic minimum score."),
			"lexical_weight":        numberSchema("Hybrid mode lexical weight. Defaults to daemon policy."),
			"semantic_weight":       numberSchema("Hybrid mode semantic weight. Defaults to daemon policy."),
			"require_both":          boolSchema("Hybrid mode: require both lexical and semantic matches."),
		}, []string{"query"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			query := strings.TrimSpace(in.Query)
			if query == "" {
				return nil, errors.New("query is required")
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			filters, err := searchFilters(in.NodeLabels, in.NodeIDs, in.PropertyFilters)
			if err != nil {
				return nil, err
			}
			mode := strings.ToLower(strings.TrimSpace(in.Mode))
			if mode == "" {
				mode = "lexical"
			}
			req := &clientv1.SearchRequest{
				SpaceId:            spaceID,
				DomainId:           domainID,
				PageSize:           a.boundedPageSize(in.PageSize),
				PageToken:          in.PageToken,
				IncludeDiagnostics: in.IncludeDiagnostics,
				AllowStale:         in.AllowStale,
				Filters:            filters,
			}
			switch mode {
			case "lexical":
				req.Mode = clientv1.SearchMode_SEARCH_MODE_LEXICAL
				req.Lexical = &clientv1.LexicalSearchOptions{Query: &query, CandidateCount: in.CandidateCount}
			case "hybrid":
				req.Mode = clientv1.SearchMode_SEARCH_MODE_HYBRID
				req.Lexical = &clientv1.LexicalSearchOptions{Query: &query, CandidateCount: in.CandidateCount}
				req.Semantic = &clientv1.SemanticSearchOptions{Query: &query, CandidateCount: in.CandidateCount, MinScore: in.MinScore}
				if strings.TrimSpace(in.SemanticRuleID) != "" {
					v := strings.TrimSpace(in.SemanticRuleID)
					req.Semantic.SemanticRuleId = &v
				}
				if strings.TrimSpace(in.EmbeddingBindingKey) != "" {
					v := strings.TrimSpace(in.EmbeddingBindingKey)
					req.Semantic.EmbeddingBindingKey = &v
				}
				if in.LexicalWeight != 0 || in.SemanticWeight != 0 || in.RequireBoth {
					req.Hybrid = &clientv1.HybridSearchOptions{LexicalWeight: in.LexicalWeight, SemanticWeight: in.SemanticWeight, RequireBoth: in.RequireBoth}
				}
			default:
				return nil, errors.New("mode must be lexical or hybrid; use mycel_semantic_search for direct semantic search")
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Search.Search(callCtx, req)
		},
	}
}

func searchFilters(labels, nodeIDs []string, propertyArgs []searchPropertyFilterArg) (*clientv1.SearchFilters, error) {
	filters := &clientv1.SearchFilters{NodeLabels: cleanStrings(labels), NodeIds: cleanStrings(nodeIDs)}
	for _, p := range propertyArgs {
		path := strings.TrimSpace(p.Path)
		if path == "" {
			return nil, errors.New("property filter path is required")
		}
		op, err := searchFilterOperator(p.Operator)
		if err != nil {
			return nil, err
		}
		filters.Properties = append(filters.Properties, &clientv1.PropertyFilter{Path: path, Operator: op, Values: cleanStrings(p.Values)})
	}
	if len(filters.NodeLabels) == 0 && len(filters.NodeIds) == 0 && len(filters.Properties) == 0 {
		return nil, nil
	}
	return filters, nil
}

func searchFilterOperator(value string) (clientv1.FilterOperator, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "equals", "equal", "eq", "=", "":
		return clientv1.FilterOperator_FILTER_OPERATOR_EQUALS, nil
	case "not_equals", "not_equal", "ne", "!=":
		return clientv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS, nil
	case "in":
		return clientv1.FilterOperator_FILTER_OPERATOR_IN, nil
	case "contains":
		return clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS, nil
	case "exists":
		return clientv1.FilterOperator_FILTER_OPERATOR_EXISTS, nil
	default:
		return clientv1.FilterOperator_FILTER_OPERATOR_UNSPECIFIED, fmt.Errorf("unsupported property filter operator %q", value)
	}
}

func newSemanticSearchTool(a *app) tool {
	type args struct {
		SpaceID             string   `json:"space_id,omitempty"`
		DomainID            string   `json:"domain_id,omitempty"`
		Query               string   `json:"query"`
		Limit               int32    `json:"limit,omitempty"`
		PageToken           string   `json:"page_token,omitempty"`
		SemanticRuleID      string   `json:"semantic_rule_id,omitempty"`
		EmbeddingBindingKey string   `json:"embedding_binding_key,omitempty"`
		MinScore            *float64 `json:"min_score,omitempty"`
		NodeLabels          []string `json:"node_labels,omitempty"`
		NodeIDs             []string `json:"node_ids,omitempty"`
	}
	return tool{
		Name:        "mycel_semantic_search",
		Description: "Run direct semantic search over committed semantic rule binding state for a Mycel space/domain.",
		InputSchema: objectSchema(map[string]any{
			"space_id":              stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":             stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"query":                 stringSchema("Natural-language text to embed for semantic retrieval."),
			"limit":                 intSchema("Maximum results to return. Capped by MYCEL_MCP_MAX_RESULTS."),
			"page_token":            stringSchema("Opaque continuation token from a prior semantic search."),
			"semantic_rule_id":      stringSchema("Optional semantic rule ID to search."),
			"embedding_binding_key": stringSchema("Optional embedding binding key. Requires semantic_rule_id when set."),
			"min_score":             numberSchema("Optional minimum score threshold."),
			"node_labels":           arrayStringSchema("All listed node labels must be present."),
			"node_ids":              arrayStringSchema("Restrict candidates to these node IDs."),
		}, []string{"query"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			query := strings.TrimSpace(in.Query)
			if query == "" {
				return nil, errors.New("query is required")
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			req := &clientv1.SemanticSearchRequest{SpaceId: spaceID, DomainId: domainID, Query: query, Limit: a.boundedPageSize(in.Limit), MinScore: in.MinScore}
			if strings.TrimSpace(in.PageToken) != "" {
				v := strings.TrimSpace(in.PageToken)
				req.PageToken = &v
			}
			if strings.TrimSpace(in.SemanticRuleID) != "" {
				v := strings.TrimSpace(in.SemanticRuleID)
				req.SemanticRuleId = &v
			}
			if strings.TrimSpace(in.EmbeddingBindingKey) != "" {
				v := strings.TrimSpace(in.EmbeddingBindingKey)
				req.EmbeddingBindingKey = &v
			}
			labels := cleanStrings(in.NodeLabels)
			nodeIDs := cleanStrings(in.NodeIDs)
			if len(labels) > 0 || len(nodeIDs) > 0 {
				req.Filter = &clientv1.SemanticEligibilityFilter{NodeLabels: labels, NodeIds: nodeIDs}
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Semantic.SemanticSearch(callCtx, req)
		},
	}
}

func newListSemanticRulesTool(a *app) tool {
	type args struct {
		SpaceID         string `json:"space_id,omitempty"`
		DomainID        string `json:"domain_id,omitempty"`
		PageSize        int32  `json:"page_size,omitempty"`
		PageToken       string `json:"page_token,omitempty"`
		IncludeDisabled bool   `json:"include_disabled,omitempty"`
	}
	return tool{
		Name:        "mycel_list_semantic_rules",
		Description: "List searchable semantic generation rules available to the authenticated caller for a Mycel space/domain.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":         stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":        stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"include_disabled": boolSchema("Include disabled semantic rules."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Semantic.ListSemanticRules(callCtx, &clientv1.ListSemanticRulesRequest{SpaceId: spaceID, DomainId: domainID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, IncludeDisabled: in.IncludeDisabled})
		},
	}
}

func newExecuteGQLReadOnlyTool(a *app) tool {
	type args struct {
		SpaceID   string         `json:"space_id,omitempty"`
		DomainID  string         `json:"domain_id,omitempty"`
		Query     string         `json:"query"`
		Params    map[string]any `json:"params,omitempty"`
		PageSize  int32          `json:"page_size,omitempty"`
		PageToken string         `json:"page_token,omitempty"`
	}
	return tool{
		Name:        "mycel_execute_gql_readonly",
		Description: "Execute textual Mycel GQL inside a read-only transaction. Mutations are expected to be rejected by the daemon because the transaction is read-only.",
		InputSchema: objectSchema(map[string]any{
			"space_id":   stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":  stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"query":      stringSchema("Textual Mycel GQL query to execute in a read-only transaction."),
			"params":     objectFreeformSchema("Optional GQL parameter map."),
			"page_size":  intSchema("Maximum rows to return. Capped by MYCEL_MCP_MAX_RESULTS."),
			"page_token": stringSchema("Opaque continuation token from a prior query."),
		}, []string{"query"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			query := strings.TrimSpace(in.Query)
			if query == "" {
				return nil, errors.New("query is required")
			}
			params, err := structParams(in.Params)
			if err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Query.ExecuteGQL(callCtx, &clientv1.ExecuteGQLRequest{TransactionId: txID, Query: query, Params: params, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken})
			})
		},
	}
}

func newExplainGQLTool(a *app) tool {
	type args struct {
		SpaceID  string         `json:"space_id,omitempty"`
		DomainID string         `json:"domain_id,omitempty"`
		Query    string         `json:"query"`
		Params   map[string]any `json:"params,omitempty"`
	}
	return tool{
		Name:        "mycel_explain_gql",
		Description: "Plan textual Mycel GQL inside a read-only transaction and return query diagnostics without executing graph reads or mutations.",
		InputSchema: objectSchema(map[string]any{
			"space_id":  stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id": stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"query":     stringSchema("Textual Mycel GQL query to explain."),
			"params":    objectFreeformSchema("Optional GQL parameter map."),
		}, []string{"query"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			query := strings.TrimSpace(in.Query)
			if query == "" {
				return nil, errors.New("query is required")
			}
			params, err := structParams(in.Params)
			if err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Query.ExplainGQL(callCtx, &clientv1.ExplainGQLRequest{TransactionId: txID, Query: query, Params: params})
			})
		},
	}
}

func newGetNodeTool(a *app) tool {
	type args struct {
		SpaceID    string `json:"space_id,omitempty"`
		DomainID   string `json:"domain_id,omitempty"`
		NodeID     string `json:"node_id"`
		AllowStale bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_get_node",
		Description: "Get one graph node by ID inside a read-only transaction.",
		InputSchema: objectSchema(map[string]any{
			"space_id":    stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":   stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"node_id":     stringSchema("Node ID to read."),
			"allow_stale": boolSchema("Request stale reads when the daemon supports them."),
		}, []string{"node_id"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			nodeID := strings.TrimSpace(in.NodeID)
			if nodeID == "" {
				return nil, errors.New("node_id is required")
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Graph.GetNode(callCtx, &clientv1.GetNodeRequest{TransactionId: txID, NodeId: nodeID, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newListNodesTool(a *app) tool {
	type args struct {
		SpaceID    string `json:"space_id,omitempty"`
		DomainID   string `json:"domain_id,omitempty"`
		PageSize   int32  `json:"page_size,omitempty"`
		PageToken  string `json:"page_token,omitempty"`
		AllowStale bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_list_nodes",
		Description: "List graph nodes in a domain inside a read-only transaction. Prefer search or GQL for selective retrieval.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":    stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":   stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"allow_stale": boolSchema("Request stale reads when the daemon supports them."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Graph.ListNodes(callCtx, &clientv1.ListNodesRequest{TransactionId: txID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newListEdgesTool(a *app) tool {
	type args struct {
		SpaceID    string `json:"space_id,omitempty"`
		DomainID   string `json:"domain_id,omitempty"`
		PageSize   int32  `json:"page_size,omitempty"`
		PageToken  string `json:"page_token,omitempty"`
		AllowStale bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_list_edges",
		Description: "List graph edges in a domain inside a read-only transaction. Prefer GQL for selective relationship traversal.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":    stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":   stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"allow_stale": boolSchema("Request stale reads when the daemon supports them."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Graph.ListEdges(callCtx, &clientv1.ListEdgesRequest{TransactionId: txID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newListChildrenTool(a *app) tool {
	type args struct {
		SpaceID      string `json:"space_id,omitempty"`
		DomainID     string `json:"domain_id,omitempty"`
		ParentNodeID string `json:"parent_node_id"`
		AllowStale   bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_list_children",
		Description: "List ordered contains edges from a parent node to its children inside a read-only transaction.",
		InputSchema: objectSchema(map[string]any{
			"space_id":       stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":      stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"parent_node_id": stringSchema("Parent node ID."),
			"allow_stale":    boolSchema("Request stale reads when the daemon supports them."),
		}, []string{"parent_node_id"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			parentID := strings.TrimSpace(in.ParentNodeID)
			if parentID == "" {
				return nil, errors.New("parent_node_id is required")
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Graph.ListChildren(callCtx, &clientv1.ListChildrenRequest{TransactionId: txID, ParentNodeId: parentID, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newGetParentTool(a *app) tool {
	type args struct {
		SpaceID     string `json:"space_id,omitempty"`
		DomainID    string `json:"domain_id,omitempty"`
		ChildNodeID string `json:"child_node_id"`
		AllowStale  bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_get_parent",
		Description: "Get the contains edge from a child node to its parent, if one exists, inside a read-only transaction.",
		InputSchema: objectSchema(map[string]any{
			"space_id":      stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":     stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"child_node_id": stringSchema("Child node ID."),
			"allow_stale":   boolSchema("Request stale reads when the daemon supports them."),
		}, []string{"child_node_id"}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			childID := strings.TrimSpace(in.ChildNodeID)
			if childID == "" {
				return nil, errors.New("child_node_id is required")
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Graph.GetParent(callCtx, &clientv1.GetParentRequest{TransactionId: txID, ChildNodeId: childID, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newListTagsTool(a *app) tool {
	type args struct {
		SpaceID    string `json:"space_id,omitempty"`
		DomainID   string `json:"domain_id,omitempty"`
		PageSize   int32  `json:"page_size,omitempty"`
		PageToken  string `json:"page_token,omitempty"`
		AllowStale bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_list_tags",
		Description: "List known canonical tags visible in a domain read transaction.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":    stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":   stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"allow_stale": boolSchema("Request stale reads when the daemon supports them."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Metadata.ListTags(callCtx, &clientv1.ListTagsRequest{TransactionId: txID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newListPropertyNamesTool(a *app) tool {
	type args struct {
		SpaceID    string `json:"space_id,omitempty"`
		DomainID   string `json:"domain_id,omitempty"`
		PageSize   int32  `json:"page_size,omitempty"`
		PageToken  string `json:"page_token,omitempty"`
		AllowStale bool   `json:"allow_stale,omitempty"`
	}
	return tool{
		Name:        "mycel_list_property_names",
		Description: "List known canonical custom property names visible in a domain read transaction.",
		InputSchema: paginationSchema(map[string]any{
			"space_id":    stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id":   stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
			"allow_stale": boolSchema("Request stale reads when the daemon supports them."),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			return a.withReadTransaction(ctx, in.SpaceID, in.DomainID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				return client.Metadata.ListPropertyNames(callCtx, &clientv1.ListPropertyNamesRequest{TransactionId: txID, PageSize: a.boundedPageSize(in.PageSize), PageToken: in.PageToken, ReadOptions: &clientv1.ReadOptions{AllowStale: in.AllowStale}})
			})
		},
	}
}

func newGetLexicalIndexStatusTool(a *app) tool {
	type args struct {
		SpaceID  string `json:"space_id,omitempty"`
		DomainID string `json:"domain_id,omitempty"`
	}
	return tool{
		Name:        "mycel_get_lexical_index_status",
		Description: "Return caller-visible lexical index freshness for a Mycel space/domain.",
		InputSchema: objectSchema(map[string]any{
			"space_id":  stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID when omitted."),
			"domain_id": stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID when omitted."),
		}, nil),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in args
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			client, err := a.mycelClient(ctx)
			if err != nil {
				return nil, err
			}
			callCtx, cancel := client.AuthCallContext(ctx)
			defer cancel()
			return client.Search.GetLexicalIndexStatus(callCtx, &clientv1.GetLexicalIndexStatusRequest{SpaceId: spaceID, DomainId: domainID})
		},
	}
}

func structParams(values map[string]any) (map[string]*structpb.Value, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]*structpb.Value, len(values))
	for k, v := range values {
		value, err := structpb.NewValue(v)
		if err != nil {
			return nil, fmt.Errorf("invalid param %q: %w", k, err)
		}
		out[k] = value
	}
	return out, nil
}

func cleanStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func decodeJSON(raw json.RawMessage, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(out); err != nil {
		return err
	}
	return nil
}

func decodeArgs(raw json.RawMessage, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	return nil
}

func resultText(value any) (string, error) {
	if value == nil {
		return "{}", nil
	}
	if msg, ok := value.(proto.Message); ok {
		data, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false, Multiline: true, Indent: "  "}.Marshal(msg)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	if text, ok := value.(string); ok {
		return text, nil
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func emptySchema() map[string]any {
	return objectSchema(nil, nil)
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func objectFreeformSchema(description string) map[string]any {
	return map[string]any{"type": "object", "description": description, "additionalProperties": true}
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func enumStringSchema(values []string, description string) map[string]any {
	return map[string]any{"type": "string", "enum": values, "description": description}
}

func boolSchema(description string) map[string]any {
	return map[string]any{"type": "boolean", "description": description}
}

func intSchema(description string) map[string]any {
	return map[string]any{"type": "integer", "description": description}
}

func numberSchema(description string) map[string]any {
	return map[string]any{"type": "number", "description": description}
}

func arrayStringSchema(description string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": description}
}

func propertyFiltersSchema() map[string]any {
	return map[string]any{
		"type":        "array",
		"description": "Optional hard property filters for search candidates.",
		"items": objectSchema(map[string]any{
			"path":     stringSchema("Dot-separated property path."),
			"operator": enumStringSchema([]string{"equals", "not_equals", "in", "contains", "exists"}, "Filter operator."),
			"values":   arrayStringSchema("String values used by the operator."),
		}, []string{"path", "operator"}),
	}
}

func paginationSchema(extra map[string]any) map[string]any {
	props := map[string]any{
		"page_size":  intSchema("Maximum results to return. Capped by MYCEL_MCP_MAX_RESULTS."),
		"page_token": stringSchema("Opaque continuation token from a prior call."),
	}
	for k, v := range extra {
		props[k] = v
	}
	return objectSchema(props, nil)
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
	a := newAppFromEnv()
	defer func() { _ = a.close() }()
	if err := newServer(a).serve(os.Stdin, os.Stdout); err != nil {
		log.Printf("%s: %v", serverName, err)
		os.Exit(1)
	}
}
