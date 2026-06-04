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
		"server_url":  "nats://localhost:4222",
		"bucket":      "DATA",
		"root_tenant": "test_bucket",
	}

	configBytes, err := json.Marshal(minimalConfig)
	require.NoError(t, err)

	// Create a minimal manager for testing
	manager := &plugins.Manager{
		Store: inmem.New(),
	}

	// Validate the configuration. These gate a *Config deref below, so use
	// require: if Validate fails (e.g. NATS is unreachable) the test must STOP
	// here, not fall through and nil-deref-panic the whole test binary.
	validatedConfig, err := factory.Validate(manager, configBytes)
	require.NoError(t, err)
	require.NotNil(t, validatedConfig)

	// Check that it's the right type
	config, ok := validatedConfig.(*Config)
	require.True(t, ok)
	assert.Equal(t, "nats://localhost:4222", config.ServerURL)
	assert.Equal(t, "test_bucket", config.RootTenant)
}

func TestPluginFactory_New(t *testing.T) {
	factory := NewPluginFactory()

	config := DefaultConfig()
	config.ServerURL = "nats://localhost:4222"
	config.Bucket = "POLICY_DATA"
	config.RootTenant = "test_bucket"

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
			name: "valid minimal config with root tenant",
			config: map[string]interface{}{
				"server_url":  "nats://localhost:4222",
				"bucket":      "DATA",
				"root_tenant": "test_tenant",
			},
			expectError: false,
		},
		{
			name: "missing server_url",
			config: map[string]interface{}{
				"bucket": "DATA",
			},
			expectError: true,
		},
		{
			name: "missing bucket",
			config: map[string]interface{}{
				"server_url": "nats://localhost:4222",
			},
			expectError: true,
		},
		{
			name: "valid config with max_bucket_watchers",
			config: map[string]interface{}{
				"server_url":          "nats://localhost:4222",
				"bucket":              "DATA",
				"max_bucket_watchers": 5,
			},
			expectError: false,
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
	config.Bucket = "POLICY_DATA"
	config.RootTenant = "test_bucket"

	// Note: Since we can't test actual NATS connectivity without a NATS server,
	// this test focuses on the plugin architecture and configuration validation

	manager := &plugins.Manager{
		Store: inmem.New(),
	}

	// Validate config
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	// require: these gate the *Plugin deref below; a failure (e.g. NATS down)
	// must stop the test rather than nil-deref-panic the whole binary.
	validatedConfig, err := factory.Validate(manager, configBytes)
	require.NoError(t, err)
	require.NotNil(t, validatedConfig)

	// Verify the factory has the right store (original store, not a composite)
	assert.Equal(t, manager.Store, factory.Store())

	// Create plugin
	plugin := factory.New(manager, validatedConfig)
	require.NotNil(t, plugin)

	natsPlugin, ok := plugin.(*Plugin)
	require.True(t, ok)
	assert.NotNil(t, natsPlugin.bucketDataManager)

	logger.Info("Data injection architecture test completed successfully")
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, "nats://localhost:4222", config.ServerURL)
	assert.Equal(t, 10, config.MaxBucketsWatchers)
	assert.NotZero(t, config.TTL)
	assert.NotZero(t, config.RefreshInterval)
	assert.Equal(t, "", config.RootTenant)
	// Bucket has no implicit default: it is mandatory and must be set explicitly.
	assert.Equal(t, "", config.Bucket)
}

// Note: Integration tests with actual NATS server would go in a separate file
// with build tags (e.g., +build integration) and would require a running NATS server with JetStream enabled.
