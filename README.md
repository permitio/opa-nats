# NATS Plugin for OPA

[![Go Reference](https://pkg.go.dev/badge/github.com/permitio/opa-nats/pkg/natsstore.svg)](https://pkg.go.dev/github.com/permitio/opa-nats/pkg/natsstore)
[![Go Report Card](https://goreportcard.com/badge/github.com/permitio/opa-nats)](https://goreportcard.com/report/github.com/permitio/opa-nats)
[![CI](https://github.com/permitio/opa-nats/actions/workflows/pr_checks.yml/badge.svg)](https://github.com/permitio/opa-nats/actions/workflows/pr_checks.yml)

This plugin provides an LRU cache backed by NATS Key-Value store for OPA. It's not a full storage implementation - just a smart caching layer that overrides the runtime store for specific paths.

## Project Layout

This project follows the [golang-standards/project-layout](https://github.com/golang-standards/project-layout) convention. The main public library code is in [`pkg/natsstore`](pkg/natsstore/). CI, configs, and documentation are at the root.

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
    bucket: "schemas"
    cache_size: 1000
    ttl: "10m"
    refresh_interval: "30s"
    watch_prefix: ""
    max_reconnect_attempts: 10
    reconnect_wait: "2s"
    
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
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `server_url` | string | `"nats://localhost:4222"` | NATS server URL |
| `bucket` | string | `"schemas"` | K/V bucket name |
| `cache_size` | int | `1000` | Maximum number of entries in LRU cache |
| `ttl` | duration | `"10m"` | TTL for cache entries |
| `refresh_interval` | duration | `"30s"` | How often to refresh cache from NATS |
| `watch_prefix` | string | `""` | Prefix to watch for K/V changes |
| `max_reconnect_attempts` | int | `10` | Maximum reconnection attempts |
| `reconnect_wait` | duration | `"2s"` | Wait time between reconnection attempts |
| `credentials` | string | `""` | Path to NATS credentials file |
| `token` | string | `""` | NATS token for authentication |
| `username` | string | `""` | NATS username |
| `password` | string | `""` | NATS password |
| `tls_cert` | string | `""` | Path to TLS certificate |
| `tls_key` | string | `""` | Path to TLS private key |
| `tls_ca_cert` | string | `""` | Path to TLS CA certificate |
| `tls_insecure` | bool | `false` | Skip TLS certificate verification |
| `handled_paths_regex` | []string | `[]` | List of regex patterns for handled paths. If empty, uses default paths. |

## Example: Regex Handled Paths

To use regex for handled paths, add the `handled_paths_regex` field to your plugin config:

```yaml
plugins:
  nats:
    server_url: "nats://localhost:4222"
    bucket: "schemas"
    cache_size: 1000
    ttl: "10m"
    handled_paths_regex:
      - "^data/schemas($|/.*)"
      - "^data/resource_types($|/.*)"
      - "^data/relationships($|/.*)"
      - "^data/role_assignments($|/.*)"
      - "^data/custom_prefix($|/.*)"
```

If `handled_paths_regex` is omitted or empty, the default paths will be used.

## Examples

See [`examples/handledpaths`](examples/handledpaths/) for a minimal Go example showing how to configure and use the plugin with `handled_paths_regex` and OPA integration.

## Usage

### 1. Import the Library

```go
import "github.com/permitio/opa-nats/pkg/natsstore"
```

### 2. Integrate with OPA

The recommended way to use this plugin with OPA is to provide your plugin configuration in a JSON or YAML file and set the `ConfigFile` field of `runtime.Params` when starting OPA. See [`examples/handledpaths`](examples/handledpaths/) for a complete example.

**Example:**

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/open-policy-agent/opa/v1/runtime"
	natsstore "github.com/permitio/opa-nats/pkg/natsstore"
)

func main() {
	pluginConfig := &natsstore.Config{
		ServerURL:         "nats://localhost:4222",
		Bucket:            "schemas",
		CacheSize:         100,
		TTL:               natsstore.Duration(5 * time.Minute),
		HandledPathsRegex: []string{"^data/schemas($|/.*)", "^data/custom($|/.*)"},
	}

	configBytes, err := json.Marshal(pluginConfig)
	if err != nil {
		log.Fatalf("Failed to marshal config: %v", err)
	}
	var configMap map[string]interface{}
	if err := json.Unmarshal(configBytes, &configMap); err != nil {
		log.Fatalf("Failed to unmarshal config to map: %v", err)
	}

	opaConfig := map[string]interface{}{
		"plugins": map[string]interface{}{
			natsstore.PluginName: configMap,
		},
	}

	tmpfile, err := os.CreateTemp("", "opa-config-*.json")
	if err != nil {
		log.Fatalf("Failed to create temp config file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	encoder := json.NewEncoder(tmpfile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(opaConfig); err != nil {
		log.Fatalf("Failed to write config to file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatalf("Failed to close config file: %v", err)
	}

	ctx := context.Background()
	params := runtime.Params{
		ConfigFile: tmpfile.Name(),
	}
	_, err = runtime.NewRuntime(ctx, params)
	if err != nil {
		log.Fatalf("Failed to start OPA runtime: %v", err)
	}
}
```

### 3. Configure OPA

Create an OPA configuration file that includes the plugin (see above for YAML or JSON example).

### 4. Start OPA

```bash
./permit-opa run --server --config-file opa-config.yaml
```

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

```bash
go build -o ./opa-nats .
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