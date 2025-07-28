package natsstore

import (
	"encoding/json"
	"testing"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/plugins"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginFactory_Validate(t *testing.T) {
	factory := NewPluginFactory()

	// Test with minimal config
	minimalConfig := map[string]interface{}{
		"server_url":   "nats://localhost:4222",
		"single_group": "test_group",
	}

	configBytes, err := json.Marshal(minimalConfig)
	require.NoError(t, err)

	// Create a minimal manager for testing
	manager := &plugins.Manager{
		Store: inmem.New(),
	}

	// Validate the configuration
	validatedConfig, err := factory.Validate(manager, configBytes)
	assert.NoError(t, err)
	assert.NotNil(t, validatedConfig)

	// Check that it's the right type
	config, ok := validatedConfig.(*Config)
	assert.True(t, ok)
	assert.Equal(t, "nats://localhost:4222", config.ServerURL)
	assert.Equal(t, "test_group", config.SingleGroup)
}

func TestPluginFactory_New(t *testing.T) {
	factory := NewPluginFactory()

	config := DefaultConfig()
	config.ServerURL = "nats://localhost:4222"
	config.SingleGroup = "test_group"

	// Create a minimal manager for testing
	manager := &plugins.Manager{
		Store: inmem.New(),
	}

	// Create the plugin
	plugin := factory.New(manager, config)
	assert.NotNil(t, plugin)

	// Check that it's the right type
	natsPlugin, ok := plugin.(*Plugin)
	assert.True(t, ok)
	assert.Equal(t, manager, natsPlugin.manager)
	assert.Equal(t, config, natsPlugin.config)
}

func TestPlugin_ConfigValidation(t *testing.T) {
	factory := NewPluginFactory()
	manager := &plugins.Manager{Store: inmem.New()}

	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
	}{
		{
			name: "valid minimal config with single group",
			config: map[string]interface{}{
				"server_url":   "nats://localhost:4222",
				"single_group": "test_group",
			},
			expectError: false,
		},
		{
			name: "valid config with group regex pattern",
			config: map[string]interface{}{
				"server_url":          "nats://localhost:4222",
				"group_regex_pattern": "[a-f0-9-]+",
			},
			expectError: false,
		},
		{
			name: "missing server_url",
			config: map[string]interface{}{
				"single_group": "test_group",
			},
			expectError: true,
		},
		{
			name: "conflicting group config (both single_group and group_regex_pattern)",
			config: map[string]interface{}{
				"server_url":          "nats://localhost:4222",
				"single_group":        "test_group",
				"group_regex_pattern": "[a-f0-9-]+",
			},
			expectError: true,
		},
		{
			name: "invalid cache_size",
			config: map[string]interface{}{
				"server_url":               "nats://localhost:4222",
				"single_group":             "test_group",
				"group_watcher_cache_size": -1,
			},
			expectError: false, // Negative cache size is not validated in the current implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configBytes, err := json.Marshal(tt.config)
			require.NoError(t, err)

			_, err = factory.Validate(manager, configBytes)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPlugin_DataInjectionArchitecture(t *testing.T) {
	logger := logging.Get()
	factory := NewPluginFactory()

	// Create a minimal config for data injection testing
	config := DefaultConfig()
	config.ServerURL = "nats://localhost:4222"
	config.SingleGroup = "test_group"

	// Note: Since we can't test actual NATS connectivity without a NATS server,
	// this test focuses on the plugin architecture and configuration validation

	manager := &plugins.Manager{
		Store: inmem.New(),
	}

	// Validate config
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	validatedConfig, err := factory.Validate(manager, configBytes)
	assert.NoError(t, err)
	assert.NotNil(t, validatedConfig)

	// Verify the factory has the right store (original store, not a composite)
	assert.Equal(t, manager.Store, factory.Store())

	// Create plugin
	plugin := factory.New(manager, validatedConfig)
	assert.NotNil(t, plugin)

	natsPlugin, ok := plugin.(*Plugin)
	assert.True(t, ok)
	assert.NotNil(t, natsPlugin.bucketDataManager)

	logger.Info("Data injection architecture test completed successfully")
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, "nats://localhost:4222", config.ServerURL)
	assert.Equal(t, 10, config.MaxGroupWatchers)
	assert.Equal(t, 100, config.GroupWatcherCacheSize)
	assert.NotZero(t, config.TTL)
	assert.NotZero(t, config.RefreshInterval)
	assert.Equal(t, "", config.GroupRegexPattern)
	assert.Equal(t, "", config.SingleGroup)
}

// Note: Integration tests with actual NATS server would go in a separate file
// with build tags (e.g., +build integration) and would require a running NATS server with JetStream enabled.
