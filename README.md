# mycel-mcp

Experimental Model Context Protocol (MCP) server for MycelDB.

This first version is intentionally **read-only**. It lets MCP clients use a running `myceld` daemon for identity/capability discovery, space/domain discovery, schema inspection, lexical/hybrid/semantic search, read-only GQL, graph reads, and metadata catalog discovery.

## Status

- Transport: MCP over stdio, newline-delimited JSON-RPC.
- Implementation: Go, using `github.com/myceldb/mycel-go-sdk`.
- Read tools are always available after authentication.
- Graph write tools are available only when write mode is enabled.
- Admin tools: not exposed in this first version.
- Local UX: `login`, `logout`, `configure`, `doctor`, and `serve` commands.

## Build

From this directory:

```sh
go build -o bin/mycel-mcp .
```

The repository workspace includes this module in `../go.work`, so local builds use the sibling `../mycel-go-sdk` checkout.

## Configure

For local desktop use, prefer the built-in login/configuration commands so passwords do not need to be placed in an MCP client config file.

```sh
# Store a Mycel refresh token securely. On macOS this uses Keychain.
bin/mycel-mcp login

# Choose default space/domain IDs interactively.
bin/mycel-mcp configure

# Diagnose daemon connectivity, auth, and selected defaults.
bin/mycel-mcp doctor

# Optional: enable mutating graph tools.
bin/mycel-mcp configure --enable-writes

# Optional: disable mutating graph tools again.
bin/mycel-mcp configure --disable-writes

# Run the stdio MCP server. This is also the default when no command is supplied.
bin/mycel-mcp serve
```

Non-secret local config is written to:

```text
~/.config/mycel-mcp/config.json
```

Set `MYCEL_MCP_CONFIG=/path/to/config.json` to use another path. Environment variables override local config when serving.

The local config stores values such as daemon address, username, default space/domain, and max result limits. It does **not** store plaintext passwords. On macOS, `mycel-mcp login` stores the refresh token in Keychain under the `mycel-mcp` service. On other platforms, the current fallback is a `0600` credentials file beside the config file.

The server also reads Mycel SDK environment variables:

| Variable | Meaning |
| --- | --- |
| `MYCELD_GRPC_ADDR` | myceld gRPC address. Defaults to `127.0.0.1:9091`. |
| `MYCEL_USERNAME` / `MYCEL_PASSWORD` | Optional principal login credentials. Useful for disposable demos, but avoid for long-lived desktop config. |
| `MYCEL_ACCESS_TOKEN` | Optional bearer access token. |
| `MYCEL_REFRESH_TOKEN` | Optional durable refresh token. If supplied without an access token, `mycel-mcp` refreshes on first daemon use. |
| `MYCELD_TLS`, `MYCELD_TLS_CA_FILE`, `MYCELD_TLS_SERVER_NAME`, `MYCELD_TLS_CLIENT_CERT_FILE`, `MYCELD_TLS_CLIENT_KEY_FILE` | TLS/mTLS settings supported by the Go SDK. |

MCP-specific environment overrides:

| Variable | Meaning |
| --- | --- |
| `MYCEL_MCP_CONFIG` | Optional local config file path. |
| `MYCEL_MCP_DEFAULT_SPACE_ID` | Default space for tools that need a space. |
| `MYCEL_MCP_DEFAULT_DOMAIN_ID` | Default domain for tools that need a domain. |
| `MYCEL_MCP_MAX_RESULTS` | Default and maximum page size for tool calls. Defaults to `25`, hard-capped at `200`. |
| `MYCEL_MCP_WRITE_ENABLED` | Enable mutating graph tools when truthy. Equivalent to `mycel-mcp configure --enable-writes`. |
| `MYCEL_MCP_ADMIN_ENABLED` | Reserved for future admin tools. Currently reported by `mycel_mcp_config` only. |

## Example MCP client config

After `mycel-mcp login` and `mycel-mcp configure`, Claude Desktop or another local MCP client only needs to start the server:

```json
{
  "mcpServers": {
    "mycel": {
      "command": "/path/to/mycel-mcp/bin/mycel-mcp",
      "args": ["serve"]
    }
  }
}
```

For quick disposable local development, environment credentials still work:

```json
{
  "mcpServers": {
    "mycel": {
      "command": "/path/to/mycel-mcp/bin/mycel-mcp",
      "args": ["serve"],
      "env": {
        "MYCELD_GRPC_ADDR": "127.0.0.1:9091",
        "MYCEL_USERNAME": "alice",
        "MYCEL_PASSWORD": "secret",
        "MYCEL_MCP_DEFAULT_SPACE_ID": "<space-id>",
        "MYCEL_MCP_DEFAULT_DOMAIN_ID": "<domain-id>"
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

### Graph writes

These tools are listed only when write mode is enabled with `mycel-mcp configure --enable-writes` or `MYCEL_MCP_WRITE_ENABLED=true`:

- `mycel_create_node`
- `mycel_upsert_node`
- `mycel_create_edge`

Each write runs in a read-write transaction and returns the graph result plus commit metadata. Pass an optional non-secret `idempotency_key` for durable retry safety.

### Metadata catalog

- `mycel_list_tags`
- `mycel_list_property_names`

## Three-node test cluster

For local MCP testing against a real three-node Mycel raft cluster, use the helper script:

```sh
scripts/test-cluster.sh start
```

It builds a local Mycel image, starts the existing Mycel Docker Compose cluster fixture with three `myceld` nodes, exposes node A/B/C on `127.0.0.1:19091`, `127.0.0.1:19092`, and `127.0.0.1:19093`, and configures bootstrap admin credentials. The high ports avoid colliding with a normal local standalone daemon on `127.0.0.1:9091`.

```text
username: admin
password: password
```

Useful commands:

```sh
scripts/test-cluster.sh status
scripts/test-cluster.sh logs
scripts/test-cluster.sh env
scripts/test-cluster.sh stop
```

After the cluster is ready, you can log `mycel-mcp` into node A:

```sh
bin/mycel-mcp login --daemon-addr 127.0.0.1:19091 --username admin --password password
bin/mycel-mcp doctor
```

The helper is destructive by default for its own Compose project and volumes. Set `MYCEL_TEST_RESET=false` to preserve existing test-cluster volumes. Set `MYCEL_TEST_ADMIN_PASSWORD` to override the default password.

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
