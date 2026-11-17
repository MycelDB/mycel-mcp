package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
)

func TestToolsListIncludesReadOnlyMVPTools(t *testing.T) {
	s := newServer(&app{cfg: config{MaxResults: defaultMaxResults}})
	id := json.RawMessage(`1`)
	res := s.handleRequest(context.Background(), rpcRequest{JSONRPC: "2.0", ID: &id, Method: "tools/list"})
	if res.Error != nil {
		t.Fatalf("tools/list returned error: %+v", res.Error)
	}
	result, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", res.Result)
	}
	items, ok := result["tools"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("tools/list did not return tools: %#v", result)
	}
	seen := map[string]bool{}
	for _, item := range items {
		tool, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("unexpected tool item type %T", item)
		}
		seen[tool["name"].(string)] = true
	}
	for _, name := range []string{"mycel_whoami", "mycel_search", "mycel_semantic_search", "mycel_execute_gql_readonly", "mycel_get_node", "mycel_list_property_names"} {
		if !seen[name] {
			t.Fatalf("expected tool %s in tools/list", name)
		}
	}
}

func TestSearchToolSchemaExposesSemanticConsistencyInputs(t *testing.T) {
	tool := newSearchTool(&app{cfg: config{MaxResults: defaultMaxResults}})
	props, ok := tool.InputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("search schema properties missing: %#v", tool.InputSchema)
	}
	for _, name := range []string{"semantic_consistency", "semantic_stale_behavior", "min_required_graph_revision", "max_semantic_revision_lag", "max_semantic_staleness"} {
		if _, ok := props[name]; !ok {
			t.Fatalf("mycel_search schema missing %s", name)
		}
	}
}

func TestSearchSemanticConsistencyPolicy(t *testing.T) {
	if policy, err := searchSemanticConsistencyPolicy("", "", 0, 0, ""); err != nil || policy != nil {
		t.Fatalf("empty policy = %#v, %v; want nil, nil", policy, err)
	}
	bounded, err := searchSemanticConsistencyPolicy("bounded-stale", "serve-last-good", 0, 25, "2m")
	if err != nil {
		t.Fatal(err)
	}
	if bounded.Consistency != clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BOUNDED_STALE || bounded.StaleBehavior != clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SERVE_LAST_GOOD || bounded.MaxRevisionLag != 25 || bounded.MaxStaleness.AsDuration() != 2*time.Minute {
		t.Fatalf("bounded policy = %#v", bounded)
	}
	bestEffort, err := searchSemanticConsistencyPolicy("best_effort", "skip_channel", 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if bestEffort.Consistency != clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BEST_EFFORT || bestEffort.StaleBehavior != clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SKIP_CHANNEL {
		t.Fatalf("best-effort policy = %#v", bestEffort)
	}
	for _, test := range []struct {
		name                     string
		consistency              string
		staleBehavior            string
		minRequiredGraphRevision int64
		maxSemanticRevisionLag   int64
		maxSemanticStaleness     string
	}{
		{name: "strict skip", consistency: "strict", staleBehavior: "skip_channel"},
		{name: "bounded missing bound", consistency: "bounded_stale", staleBehavior: "serve_last_good"},
		{name: "read your writes missing revision", consistency: "read_your_writes", staleBehavior: "serve_last_good"},
		{name: "field without consistency", staleBehavior: "skip_channel"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := searchSemanticConsistencyPolicy(test.consistency, test.staleBehavior, test.minRequiredGraphRevision, test.maxSemanticRevisionLag, test.maxSemanticStaleness); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestConfigToolDoesNotRequireDaemon(t *testing.T) {
	a := &app{cfg: config{DefaultSpaceID: "sp", DefaultDomainID: "dom", MaxResults: 7}}
	s := newServer(a)
	id := json.RawMessage(`"call-1"`)
	params := json.RawMessage(`{"name":"mycel_mcp_config","arguments":{}}`)
	res := s.handleRequest(context.Background(), rpcRequest{JSONRPC: "2.0", ID: &id, Method: "tools/call", Params: params})
	if res.Error != nil {
		t.Fatalf("tools/call returned protocol error: %+v", res.Error)
	}
	result, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", res.Result)
	}
	if result["isError"] == true {
		t.Fatalf("config tool returned tool error: %#v", result)
	}
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	if !json.Valid([]byte(text)) {
		t.Fatalf("config tool text is not valid JSON: %s", text)
	}
}
