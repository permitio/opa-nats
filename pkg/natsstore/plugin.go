package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/plugins"
	"github.com/open-policy-agent/opa/v1/storage"
)

const (
	PluginName = "nats"
)

// PluginFactory creates and manages NATS K/V store plugin instances.
type PluginFactory struct {
	compositeStore *SimpleCompositeStore
}

// NewPluginFactory creates a new NATS K/V store plugin factory.
func NewPluginFactory() *PluginFactory {
	return &PluginFactory{}
}

func (f *PluginFactory) Store() storage.Store {
	return f.compositeStore
}

// Validate validates the plugin configuration.
func (f *PluginFactory) Validate(manager *plugins.Manager, config []byte) (any, error) {
	logger := manager.Logger()
	if logger == nil {
		logger = logging.New() // Create a default logger for testing scenarios
	}

	// Start with empty config to properly validate required fields
	pluginConfig := &Config{}

	if len(config) > 0 {
		if err := json.Unmarshal(config, pluginConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal plugin config: %w", err)
		}
	}

	// Validate required fields first
	if err := pluginConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid plugin config: %w", err)
	}

	// Apply defaults for optional fields
	f.applyDefaults(pluginConfig)

	// Validate again with defaults applied
	if err := pluginConfig.ValidateWithDefaults(); err != nil {
		return nil, fmt.Errorf("invalid plugin config: %w", err)
	}

	logger.Info("Validated NATS K/V store plugin config: %+v", pluginConfig)
	// Create the NATS cache (not a full store, just LRU cache + NATS K/V)
	natsCache, err := NewNATSCache(pluginConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS cache: %w", err)
	}

	f.compositeStore = NewSimpleCompositeStore(manager.Store, natsCache, logger, pluginConfig.HandledPathsRegex, pluginConfig.GroupRegexPattern, pluginConfig.SingleGroup)
	return pluginConfig, nil
}

// applyDefaults applies default values for optional configuration fields.
func (f *PluginFactory) applyDefaults(config *Config) {
	if config.TTL == 0 {
		config.TTL = Duration(10 * time.Minute)
	}
	if config.RefreshInterval == 0 {
		config.RefreshInterval = Duration(30 * time.Second)
	}
	if config.MaxReconnectAttempts == 0 {
		config.MaxReconnectAttempts = 10
	}
	if config.ReconnectWait == 0 {
		config.ReconnectWait = Duration(2 * time.Second)
	}
	if config.MaxGroupWatchers == 0 {
		config.MaxGroupWatchers = 10
	}
}

// New creates a new plugin instance.
func (f *PluginFactory) New(manager *plugins.Manager, config any) plugins.Plugin {
	logger := manager.Logger()
	pluginConfig, ok := config.(*Config)
	if !ok {
		logger.Error("Invalid config type for NATS K/V store plugin")
		return nil
	}

	return &Plugin{
		manager:        manager,
		config:         pluginConfig,
		compositeStore: f.compositeStore,
		logger:         logger,
	}
}

// Plugin represents the NATS K/V cache plugin instance.
type Plugin struct {
	manager        *plugins.Manager
	config         *Config
	compositeStore *SimpleCompositeStore
	logger         logging.Logger
}

// Start initializes and starts the plugin.
func (p *Plugin) Start(ctx context.Context) error {
	p.logger.Info("Starting NATS K/V cache plugin")

	if err := p.compositeStore.Start(ctx); err != nil {
		return fmt.Errorf("failed to start composite store: %w", err)
	}
	// Override the runtime store with our composite store
	p.manager.Store = p.compositeStore
	p.logger.Info("NATS plugin started successfully")
	return nil
}

// Stop shuts down the plugin.
func (p *Plugin) Stop(ctx context.Context) {
	p.logger.Info("Stopping NATS K/V cache plugin")

	if err := p.compositeStore.Stop(ctx); err != nil {
		p.logger.Error("Error stopping composite store: %v", err)
	}

	p.logger.Info("NATS plugin stopped")
}

// Reconfigure updates the plugin configuration.
func (p *Plugin) Reconfigure(ctx context.Context, config any) {
	p.logger.Info("Reconfiguring NATS K/V cache plugin")

	newConfig, ok := config.(*Config)
	if !ok {
		p.logger.Error("Invalid config type for reconfiguration")
		return
	}

	// Stop the current cache
	if err := p.compositeStore.Stop(ctx); err != nil {
		p.logger.Error("Error stopping composite store during reconfiguration: %v", err)
	}

	// Update configuration
	p.config = newConfig

	// Create and start new NATS cache with updated config
	natsCache, err := NewNATSCache(newConfig, p.logger)
	if err != nil {
		p.logger.Error("Failed to create new NATS cache during reconfiguration: %v", err)
		return
	}

	if err := natsCache.Start(ctx); err != nil {
		p.logger.Error("Failed to start new NATS cache during reconfiguration: %v", err)
		return
	}

	// Extract the original store from the current composite store (if it exists)
	var originalStore storage.Store
	if compositeStore, ok := p.manager.Store.(*SimpleCompositeStore); ok {
		originalStore = compositeStore.Store // Access embedded store
	} else {
		originalStore = p.manager.Store
	}

	// Create new composite store with the updated NATS cache
	newCompositeStore := NewSimpleCompositeStore(originalStore, natsCache, p.logger, p.config.HandledPathsRegex, p.config.GroupRegexPattern, p.config.SingleGroup)

	p.compositeStore = newCompositeStore
	p.manager.Store = newCompositeStore

	p.logger.Info("NATS K/V cache plugin reconfigured successfully")
}

// GetStats returns plugin statistics.
func (p *Plugin) GetStats() map[string]interface{} {
	if p.compositeStore == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}

	stats := p.compositeStore.natsCache.GetStats()
	stats["plugin"] = PluginName
	stats["status"] = "running"

	return stats
}
