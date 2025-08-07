# NATS Plugin for OPA

[![Go Reference](https://pkg.go.dev/badge/github.com/permitio/opa-nats/pkg/natsstore.svg)](https://pkg.go.dev/github.com/permitio/opa-nats/pkg/natsstore)
[![Go Report Card](https://goreportcard.com/badge/github.com/permitio/opa-nats)](https://goreportcard.com/report/github.com/permitio/opa-nats)
[![CI](https://github.com/permitio/opa-nats/actions/workflows/pr_checks.yml/badge.svg)](https://github.com/permitio/opa-nats/actions/workflows/pr_checks.yml)

This plugin provides an LRU cache backed by NATS Key-Value store for OPA. It's not a full storage implementation - just a smart caching layer that overrides the runtime store for specific paths.

## Project Layout

This project follows the [golang-standards/project-layout](https://github.com/golang-standards/project-layout) convention:

```
├── cmd/                         # Applications
│   └── opa-nats/               # Custom OPA binary with NATS plugin
├── pkg/                        # Public library code
│   └── natsstore/             # Core NATS store plugin package
├── examples/                   # Usage examples
│   └── handledpaths/          # Complete working example with Docker Compose
├── Dockerfile                 # Multi-platform container build
├── go.mod, go.sum            # Go modules
└── .github/                  # CI/CD configuration
```

The main public library code is in [`pkg/natsstore`](pkg/natsstore/). The [`cmd/opa-nats`](cmd/opa-nats/) directory contains a ready-to-use OPA binary with the NATS plugin pre-registered.

## Features

- **Simple LRU Cache**: Just an LRU cache with NATS K/V backing - not a full store implementation
- **Smart Path Routing**: Only caches specific paths (schemas, resource_types, etc.) in NATS
- **Automatic Updates**: Watches NATS K/V for changes and automatically updates the cache
- **TTL Support**: Configurable time-to-live for cached entries
- **Struct Embedding**: Uses Go struct embedding for clean runtime store override
- **Production Ready**: Includes connection management, error handling, and monitoring

## Configuration

The plugin can be configured through OPA's configuration system. Here's an example configuration:

```yaml
plugins:
  nats:
    server_url: "nats://localhost:4222"
    ttl: "10m"
    refresh_interval: "30s"
    max_reconnect_attempts: 10
    reconnect_wait: "2s"
    max_bucket_watchers: 10
    root_bucket: ""  # Optional - leave empty for multi-bucket mode

    # Authentication (choose one)
    credentials: "/path/to/nats.creds"
    # OR
    token: "your-token"
    # OR
    username: "user"
    password: "pass"

    # TLS (optional)
    tls_cert: "/path/to/cert.pem"
    tls_key: "/path/to/key.pem"
    tls_ca_cert: "/path/to/ca.pem"
    tls_insecure: false

services:
  authz:
    url: http://localhost:8181
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `server_url` | string | `"nats://localhost:4222"` | NATS server URL |
| `ttl` | duration | `"10m"` | TTL for cache entries |
| `refresh_interval` | duration | `"30s"` | How often to refresh cache from NATS |
| `max_reconnect_attempts` | int | `10` | Maximum reconnection attempts |
| `reconnect_wait` | duration | `"2s"` | Wait time between reconnection attempts |
| `max_bucket_watchers` | int | `10` | Maximum number of bucket watchers in LRU cache |
| `root_bucket` | string | `""` | Root bucket name for default data (optional) |
| `credentials` | string | `""` | Path to NATS credentials file |
| `token` | string | `""` | NATS token for authentication |
| `username` | string | `""` | NATS username |
| `password` | string | `""` | NATS password |
| `tls_cert` | string | `""` | Path to TLS certificate |
| `tls_key` | string | `""` | Path to TLS private key |
| `tls_ca_cert` | string | `""` | Path to TLS CA certificate |
| `tls_insecure` | bool | `false` | Skip TLS certificate verification |

## Built-in Functions

The plugin provides custom Rego built-in functions for interacting with NATS K/V:

### `nats.kv.watch_bucket(bucket_name)`

Watches a NATS K/V bucket and returns all its data. This function automatically manages bucket watchers with LRU caching.

```rego
# Watch a specific bucket
group_data := nats.kv.watch_bucket("550e8400-e29b-41d4-a716-446655440000")

# Access nested data
members := group_data.members
permissions := group_data.permissions
```

### `nats.kv.get_data(bucket_name, key)`

Retrieves a specific key from a NATS K/V bucket.

```rego
# Get specific data from a bucket
user_data := nats.kv.get_data("users", "123e4567-e89b-12d3-a456-426614174000")
```

## Examples

See [`examples/handledpaths`](examples/handledpaths/) for a complete example with:
- Custom OPA binary with NATS plugin
- Docker Compose setup with NATS server and UI
- Example Rego policies using the built-in functions
- Test data setup and configuration

## Usage

### 1. Import the Library

```go
import "github.com/permitio/opa-nats/pkg/natsstore"
```

### 2. Using the Custom Binary

The simplest way to use this plugin is with the pre-built custom OPA binary:

```go
package main

import (
	"log"

	"github.com/open-policy-agent/opa/cmd"
	"github.com/open-policy-agent/opa/v1/runtime"
	natsstore "github.com/permitio/opa-nats/pkg/natsstore"
)

func main() {
	// Register the plugin
	pluginFactory := natsstore.NewPluginFactory()
	runtime.RegisterPlugin(natsstore.PluginName, pluginFactory)

	// Run OPA with the registered plugin
	if err := cmd.RootCommand.Execute(); err != nil {
		log.Fatalf("Failed to execute OPA runtime: %v", err)
	}
}
```

### 3. Library Integration

For custom integrations, you can use the plugin as a library. See [`examples/handledpaths/main.go`](examples/handledpaths/main.go) for a complete example.

### 4. Configure OPA

Create an OPA configuration file that includes the plugin (see above for YAML or JSON example).

### 5. Start OPA

#### Using the pre-built binary:

```bash
./opa run --server --config-file opa-config.yaml
```

#### Using Docker:

```bash
docker run -p 8181:8181 -v ./config.yaml:/config.yaml opa-nats \
  run --server --config-file=/config.yaml --addr=0.0.0.0:8181
```

#### Using Docker Compose (recommended for development):

See [`examples/handledpaths/docker-compose.yaml`](examples/handledpaths/docker-compose.yaml) for a complete setup with NATS server and example data.

## How It Works

1. **Struct Embedding**: Uses Go struct embedding to cleanly override the runtime store
2. **Simple Cache**: Creates an LRU cache backed by NATS K/V (not a full store implementation)
3. **Path-Based Routing**: Only specific paths (schemas, resource_types, etc.) use the NATS cache
4. **Auto-Updates**: Watches for K/V changes and automatically updates the cache
5. **TTL Management**: Cache entries expire based on the configured TTL
6. **Transparent Operation**: All other OPA functionality (policies, etc.) works normally through the embedded store

## Store Architecture

The plugin uses a **Composite Store** pattern that properly overrides the OPA runtime store:

### Simple Cache Design

```
┌─────────────────────────────────────┐
│        SimpleCompositeStore        │
│        (Struct Embedding)          │
├─────────────────────────────────────┤
│  storage.Store (embedded)           │
│  ├─ All methods inherited           │
│  └─ Only overrides Read/Write       │
│                                     │
│  Custom routing:                    │
│  • Read/Write → path-based routing  │
│  • Everything else → embedded store │
└─────────────────────────────────────┘
          ↙                ↘
┌──────────────────────────────┐  ┌─────────────────┐
│   NATS-backed Cache Layer    │  │  Original OPA   │
│  (LRU Cache + NATS K/V)      │  │     Store       │
│  ┌───────────────┐           │  │  (embedded)     │
│  │   LRU Cache   │           │  │                 │
│  └───────────────┘           │  │  • Policies     │
│         ↓                    │  │  • Transactions │
│   NATS K/V (source of truth) │  │  • Triggers     │
└──────────────────────────────┘  └─────────────────┘
```

**How it works:**
- The LRU cache is a local, in-memory cache to avoid NATS IO for repeated accesses.
- NATS K/V is the single source of truth for the handled paths.
- On a cache miss, data is fetched from NATS and stored in the LRU cache.
- All writes go to NATS and update the cache.
- The rest of OPA's data (policies, etc.) uses the original store.

### Handled Paths

There are no hardcoded default handled paths. The set of paths routed to NATS K/V is determined entirely by the `handled_paths_regex` configuration option. If `handled_paths_regex` is empty or not set, no paths will be routed to NATS K/V.

All other paths (policies, etc.) continue using the original OPA store.

## Path Mapping

The plugin maps OPA storage paths to NATS K/V keys using dot notation:

- OPA path: `["data", "schemas", "user"]`
- NATS K/V key: `data.schemas.user`

## Monitoring

The plugin provides statistics through the OPA status API:

```bash
curl http://localhost:8181/v1/status
```

Statistics include:
- Cache size and capacity
- NATS connection status
- Number of registered triggers
- Policy count

## NATS Setup

### Requirements

- NATS Server 2.3.0+ with JetStream enabled
- K/V store functionality enabled

### Example NATS Configuration

```conf
jetstream: enabled

accounts: {
  APP: {
    jetstream: enabled
    users: [
      {user: "opa", pass: "secret", permissions: {
        subscribe: ["$JS.API.>", "$KV.>"]
        publish: ["$JS.API.>", "$KV.>"]
      }}
    ]
  }
}
```

## Development

This is a standalone Go library, maintained in the [opa-nats](https://github.com/permitio/opa-nats) repository.

### Building

#### Custom OPA Binary

```bash
go build -o opa ./cmd/opa-nats
```

#### Docker Image

```bash
docker build -t opa-nats .
```

#### Multi-platform Docker Build

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t opa-nats .
```

### Testing

```bash
go test ./... -cover
```

### Continuous Integration

This library uses GitHub Actions for CI, including linting, build, security checks, and test coverage reporting. See [.github/workflows/pr_checks.yml](.github/workflows/pr_checks.yml).

## Security Considerations

1. **Authentication**: Always use proper authentication (credentials, tokens, or user/pass)
2. **TLS**: Enable TLS for production deployments
3. **Network Security**: Ensure NATS server is properly secured and accessible only to authorized clients
4. **Bucket Permissions**: Configure appropriate K/V bucket permissions

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check NATS server URL and authentication
2. **Bucket Not Found**: Ensure the K/V bucket exists or the plugin has permissions to create it
3. **Cache Misses**: Check TTL settings and NATS connectivity
4. **Performance Issues**: Adjust cache size and refresh intervals

### Logs

The plugin uses structured logging. Enable debug logging to see detailed operation:

```yaml
plugins:
  nats:
    # ... other config
    log_level: debug
```

## License

This project is licensed under the Apache License, Version 2.0.
See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open issues or pull requests on the [opa-nats GitHub repository](https://github.com/permitio/opa-nats) for bug reports, feature requests, or improvements related to the `nats-store` library.

## Development: pre-commit hooks

This repository uses [pre-commit](https://pre-commit.com/) to enforce Go best practices and code quality. Hooks include:
- `golangci-lint` (linting)
- `go fmt` (formatting)
- `go vet` (static analysis)
- `go mod tidy` (module tidiness)
- Trailing whitespace and end-of-file checks

### Setup
1. Install pre-commit (requires Python):
   ```sh
   pip install pre-commit
   ```
2. Install the git hook scripts:
   ```sh
   pre-commit install
   ```
3. Run all hooks on all files (before pushing):
   ```sh
   pre-commit run --all-files
   ```

> All CI checks will run these hooks. Please ensure your code passes before submitting a PR.
