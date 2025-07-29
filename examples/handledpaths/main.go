package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/open-policy-agent/opa/cmd"
	"github.com/open-policy-agent/opa/v1/runtime"
	natsstore "github.com/permitio/opa-nats/pkg/natsstore"
)

// This example demonstrates how to configure the natsstore plugin with OPA
// for group management where data is injected directly into the OPA store.
func main() {
	// Example plugin config for data injection approach
	// The plugin will inject group data directly into the OPA store
	pluginConfig := &natsstore.Config{
		ServerURL: "nats://localhost:4222",
		TTL:       natsstore.Duration(5 * time.Minute),

		// Group watcher settings - MaxGroupWatchers is the LRU cache size for group watchers
		MaxBucketsWatchers: 10,
		RootBucket:         "example-bucket",
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

	// Register the natsstore plugin with OPA
	pluginFactory := natsstore.NewPluginFactory()
	runtime.RegisterPlugin(natsstore.PluginName, pluginFactory)

	if err := cmd.RootCommand.Execute(); err != nil {
		log.Fatalf("Failed to execute OPA runtime: %v", err)
	}
}
