package main

import (
	"context"
	"encoding/json"
	"testing"
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
