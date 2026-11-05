# mycel-mcp

Experimental Model Context Protocol (MCP) server for MycelDB.

This first version is intentionally **read-only**. It lets MCP clients use a running `myceld` daemon for identity/capability discovery, space/domain discovery, schema inspection, lexical/hybrid/semantic search, read-only GQL, graph reads, and metadata catalog discovery.

## Status

- Transport: MCP over stdio, newline-delimited JSON-RPC.
- Implementation: Go, using `github.com/myceldb/mycel-go-sdk`.
- Mutations: not exposed in this first version.
- Admin tools: not exposed in this first version.

## Build

From this directory:

```sh
go build -o bin/mycel-mcp .
```

The repository workspace includes this module in `../go.work`, so local builds use the sibling `../mycel-go-sdk` checkout.

## Configure

The server reads Mycel SDK environment variables:

| Variable | Meaning |
| --- | --- |
| `MYCELD_GRPC_ADDR` | myceld gRPC address. Defaults to `127.0.0.1:9091`. |
| `MYCEL_USERNAME` / `MYCEL_PASSWORD` | Optional principal login credentials. |
| `MYCEL_ACCESS_TOKEN` | Optional bearer access token. |
| `MYCEL_REFRESH_TOKEN` | Optional durable refresh token. If supplied without an access token, `mycel-mcp` refreshes on first daemon use. |
| `MYCELD_TLS`, `MYCELD_TLS_CA_FILE`, `MYCELD_TLS_SERVER_NAME`, `MYCELD_TLS_CLIENT_CERT_FILE`, `MYCELD_TLS_CLIENT_KEY_FILE` | TLS/mTLS settings supported by the Go SDK. |

MCP-specific settings:

| Variable | Meaning |
| --- | --- |
| `MYCEL_MCP_DEFAULT_SPACE_ID` | Default space for tools that need a space. |
| `MYCEL_MCP_DEFAULT_DOMAIN_ID` | Default domain for tools that need a domain. |
| `MYCEL_MCP_MAX_RESULTS` | Default and maximum page size for tool calls. Defaults to `25`, hard-capped at `200`. |
| `MYCEL_MCP_WRITE_ENABLED` | Reserved for future write tools. Currently reported by `mycel_mcp_config` only. |
| `MYCEL_MCP_ADMIN_ENABLED` | Reserved for future admin tools. Currently reported by `mycel_mcp_config` only. |

For desktop MCP clients, prefer a least-privilege service principal or refresh token over placing a human password in client config.

## Example MCP client config

```json
{
  "mcpServers": {
    "mycel": {
      "command": "/path/to/mycel-mcp/bin/mycel-mcp",
      "env": {
        "MYCELD_GRPC_ADDR": "127.0.0.1:9091",
        "MYCEL_REFRESH_TOKEN": "<refresh-token>",
        "MYCEL_MCP_DEFAULT_SPACE_ID": "<space-id>",
        "MYCEL_MCP_DEFAULT_DOMAIN_ID": "<domain-id>",
        "MYCEL_MCP_MAX_RESULTS": "25"
      }
    }
  }
}
```

For quick local development you can use username/password instead:

```json
{
  "mcpServers": {
    "mycel": {
      "command": "/path/to/mycel-mcp/bin/mycel-mcp",
      "env": {
        "MYCELD_GRPC_ADDR": "127.0.0.1:9091",
        "MYCEL_USERNAME": "alice",
        "MYCEL_PASSWORD": "secret"
      }
    }
  }
}
```

## Tools

### MCP / identity

- `mycel_mcp_config` — show non-secret server configuration.
- `mycel_whoami` — show authenticated Mycel principal.
- `mycel_get_access` — show effective roles/capabilities, optionally scoped to system, space, or domain.

### Spaces, domains, and schema

- `mycel_list_spaces`
- `mycel_get_space`
- `mycel_list_domains`
- `mycel_get_domain`
- `mycel_get_domain_schema`

### Search and semantic discovery

- `mycel_search` — lexical or hybrid search.
- `mycel_semantic_search` — direct semantic search.
- `mycel_list_semantic_rules`
- `mycel_get_lexical_index_status`

### Read-only query

- `mycel_execute_gql_readonly` — executes GQL inside a read-only transaction.
- `mycel_explain_gql` — returns GQL planning diagnostics.

### Graph reads

- `mycel_get_node`
- `mycel_list_nodes`
- `mycel_list_edges`
- `mycel_list_children`
- `mycel_get_parent`

### Metadata catalog

- `mycel_list_tags`
- `mycel_list_property_names`

## Smoke test

You can test basic MCP responses without a daemon because `tools/list` does not connect to Mycel:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  | bin/mycel-mcp
```

To test daemon auth, call `mycel_whoami` from an MCP client or send a `tools/call` request with valid Mycel credentials in the environment.

## Design notes

- Tool results are JSON text content using Mycel protobuf JSON names.
- Tool schemas reject unknown input fields to reduce accidental misuse.
- Tools that read graph/query/metadata open a daemon read-only session and transaction, then close both after the call.
- Write and admin operations should be added behind explicit opt-in flags and least-privilege guidance in a later version.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
