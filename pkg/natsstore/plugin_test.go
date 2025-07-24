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
		"single_group": "test_bucket",
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
	assert.Equal(t, "test_bucket", config.SingleGroup)
}

func TestPluginFactory_New(t *testing.T) {
	factory := NewPluginFactory()

	config := DefaultConfig()
	config.ServerURL = "nats://localhost:4222"
	config.SingleGroup = "test_bucket"

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
			name: "valid minimal config",
			config: map[string]interface{}{
				"server_url": "nats://localhost:4222",
				"bucket":     "test_bucket",
			},
			expectError: false,
		},
		{
			name: "missing server_url",
			config: map[string]interface{}{
				"bucket": "test_bucket",
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
			name: "invalid cache_size",
			config: map[string]interface{}{
				"server_url": "nats://localhost:4222",
				"bucket":     "test_bucket",
				"cache_size": -1,
			},
			expectError: true,
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

func TestCompositeStore_PathRouting(t *testing.T) {
	logger := logging.Get()
	originalStore := inmem.New()

	// Create a minimal NATS store for testing (without actual NATS connection)
	config := DefaultConfig()
	config.ServerURL = "nats://localhost:4222"
	config.SingleGroup = "test_bucket"

	// Note: We can't actually test the full NATS cache without a NATS server
	// This test focuses on the composite store routing logic

	compositeStore := NewSimpleCompositeStore(
		originalStore, // Embedded store
		nil,           // Would be a real NATS cache in production
		logger,
		[]string{
			"^data/schemas($|/.*)",
			"^data/resource_types($|/.*)",
		}, // Regex patterns for testing
		"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", // Group regex pattern for testing
		"", // No single group for testing
	)

	tests := []struct {
		name    string
		path    []string
		useNATS bool
	}{
		{
			name:    "schema path should use NATS",
			path:    []string{"data", "schemas", "user"},
			useNATS: true,
		},
		{
			name:    "resource_types path should use NATS",
			path:    []string{"data", "resource_types", "document"},
			useNATS: true,
		},
		{
			name:    "policy path should use original store",
			path:    []string{"policies", "main"},
			useNATS: false,
		},
		{
			name:    "other data path should use original store",
			path:    []string{"data", "other", "stuff"},
			useNATS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compositeStore.shouldUseNATS(tt.path)
			assert.Equal(t, tt.useNATS, result)
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, "nats://localhost:4222", config.ServerURL)
	assert.Equal(t, 100, config.GroupWatcherCacheSize)
	assert.NotZero(t, config.TTL)
	assert.NotZero(t, config.RefreshInterval)
}

// Note: Integration tests with actual NATS server would go in a separate file
// and would require a running NATS server with JetStream enabled.
