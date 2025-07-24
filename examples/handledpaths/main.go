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

// This example demonstrates how to configure the natsstore plugin with OPA
// for group management where each group is its own NATS bucket.
func main() {
	// Example plugin config for multi-bucket groups
	// Each group will become its own NATS bucket
	pluginConfig := &natsstore.Config{
		ServerURL: "nats://localhost:4222",
		TTL:       natsstore.Duration(5 * time.Minute),

		// Group watcher settings - MaxGroupWatchers is the LRU cache size for group watchers
		MaxGroupWatchers:      10,
		GroupWatcherCacheSize: 100,

		// Group regex pattern to extract group ID for both watching and bucket routing
		GroupRegexPattern: "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",

		// Handled paths regex - defines which paths should be routed to NATS
		HandledPathsRegex: []string{
			".*",    
		},
	}

	// Marshal config to map[string]interface{} for OPA
	configBytes, err := json.Marshal(pluginConfig)
	if err != nil {
		log.Fatalf("Failed to marshal config: %v", err)
	}
	var configMap map[string]interface{}
	if err := json.Unmarshal(configBytes, &configMap); err != nil {
		log.Fatalf("Failed to unmarshal config to map: %v", err)
	}

	// Prepare OPA runtime configuration
	opaConfig := map[string]interface{}{
		"plugins": map[string]interface{}{
			natsstore.PluginName: configMap,
		},
	}

	// Write config to a temp file
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

	// Register the natsstore plugin with OPA
	pluginFactory := natsstore.NewPluginFactory()
	runtime.RegisterPlugin(natsstore.PluginName, pluginFactory)

	// Start OPA runtime with config file
	ctx := context.Background()
	params := runtime.Params{
		Addrs:      &[]string{"0.0.0.0:8181"},
		ConfigFile: tmpfile.Name(),
		Logging: runtime.LoggingConfig{
			Level:  "debug",
			Format: "text",
		},
	}
	rt, err := runtime.NewRuntime(ctx, params)
	if err != nil {
		log.Fatalf("Failed to create OPA runtime: %v", err)
	}
	rt.Store = pluginFactory.Store()
	rt.Manager.Store = rt.Store
	rt.StartServer(ctx)
}
