package natsstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/plugins"
	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	PluginName             = "nats"
	WatchBucketBuiltinName = "nats.kv.watch_bucket"
	GetDataBuiltinName     = "nats.kv.get_data"
)

func getDataCacheKey(bucketName string) string {
	return fmt.Sprintf("nats.kv.%s", bucketName)
}

// PluginFactory creates and manages NATS K/V store plugin instances.
type PluginFactory struct {
	logger            logging.Logger
	bucketDataManager *BucketDataManager
	originalStore     storage.Store
}

// NewPluginFactory creates a new NATS K/V store plugin factory.
func NewPluginFactory() *PluginFactory {
	factory := &PluginFactory{}
	factory.RegisterBuiltin()
	return factory
}

func (f *PluginFactory) watchBucketBuiltin(bctx rego.BuiltinContext, inputTerm *ast.Term) (*ast.Term, error) {
	bucketName := string(inputTerm.Value.(ast.String))

	// Check if bucket is already being watched
	if f.bucketDataManager.watcherManager.HasWatcher(bucketName) {
		return ast.BooleanTerm(true), nil
	}

	// Bucket not watched yet - load data into cache and start watching
	bucketGjson, err := f.loadBucketAsGJSON(bucketName)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			return ast.BooleanTerm(false), nil
		}
		return nil, fmt.Errorf("failed to load bucket data: %w", err)
	}

	// Store gjson data in ND builtin cache for this query
	cacheKey := getDataCacheKey(bucketName)
	bctx.Cache.Put(cacheKey, bucketGjson)

	// Start watching asynchronously (no OPA store writes during query)
	go func() {
		if _, err := f.bucketDataManager.watcherManager.GetOrCreateWatcher(bucketName, f.originalStore); err != nil {
			// Log error but don't fail the query
			f.logger.Error("Failed to start watcher for bucket %s: %v", bucketName, err)
		}
	}()

	return ast.BooleanTerm(false), nil
}

func (f *PluginFactory) getDataBuiltin(bctx rego.BuiltinContext, bucketTerm *ast.Term, keyTerm *ast.Term) (*ast.Term, error) {
	bucketName := string(bucketTerm.Value.(ast.String))
	dotNotationKey := string(keyTerm.Value.(ast.String))

	// Try to get gjson data from cache first
	cacheKey := getDataCacheKey(bucketName)
	if cachedValue, ok := bctx.Cache.Get(cacheKey); ok {
		if bucketGjson, ok := cachedValue.(*gjson.Result); ok {
			result := bucketGjson.Get(dotNotationKey)
			if result.Exists() {
				return f.gjsonResultToASTTerm(result), nil
			} else {
				return ast.NullTerm(), nil
			}
		}
	}

	// Cache miss - load directly from NATS with warning
	f.logger.Warn("Warning: Cache miss for bucket %s, key %s. Loading directly from NATS.\n", bucketName, dotNotationKey)

	bucketGjson, err := f.loadBucketAsGJSON(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to load bucket data: %w", err)
	}

	// Cache the gjson data for subsequent calls in this query
	bctx.Cache.Put(cacheKey, bucketGjson)

	// Get the requested value using gjson
	result := bucketGjson.Get(dotNotationKey)
	if result.Exists() {
		return f.gjsonResultToASTTerm(result), nil
	}

	// Key not found
	return ast.NullTerm(), nil
}

func (f *PluginFactory) loadBucketAsGJSON(bucketName string) (*gjson.Result, error) {
	// Get the bucket
	kv, err := f.bucketDataManager.natsClient.getBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}

	// List all keys in the bucket
	keys, err := kv.Keys()
	if err != nil {
		// Handle empty bucket case
		if err.Error() == "nats: no keys found" {
			return &gjson.Result{Type: gjson.Null}, nil
		}
		return nil, fmt.Errorf("failed to list keys in bucket %s: %w", bucketName, err)
	}

	// Build JSON using sjson
	jsonBytes := []byte("{}")
	for _, key := range keys {
		entry, err := kv.Get(key)
		if err != nil {
			continue // Skip failed keys
		}

		jsonBytes, _ = sjson.SetRawBytes(jsonBytes, key, entry.Value())
	}

	gjsonResult := gjson.ParseBytes(jsonBytes)
	return &gjsonResult, nil
}

func (f *PluginFactory) gjsonResultToASTTerm(result gjson.Result) *ast.Term {
	switch result.Type {
	case gjson.String:
		return ast.StringTerm(result.String())
	case gjson.Number:
		return ast.NumberTerm(json.Number(result.Raw))
	case gjson.True:
		return ast.BooleanTerm(true)
	case gjson.False:
		return ast.BooleanTerm(false)
	case gjson.JSON:
		// Parse as JSON and convert to AST
		var value interface{}
		if err := json.Unmarshal([]byte(result.Raw), &value); err != nil {
			return ast.StringTerm(result.Raw) // Fallback to string
		}
		// Convert interface{} to AST term manually
		return ast.NewTerm(ast.MustInterfaceToValue(value))
	default:
		return ast.NullTerm()
	}
}

func (f *PluginFactory) RegisterBuiltin() {
	// Register nats.kv.watch_bucket builtin
	rego.RegisterBuiltin1(
		&rego.Function{
			Name:             WatchBucketBuiltinName,
			Decl:             types.NewFunction(types.Args(types.S), types.B),
			Memoize:          false,
			Nondeterministic: true,
		},
		f.watchBucketBuiltin,
	)

	// Register nats.kv.get_data builtin
	rego.RegisterBuiltin2(
		&rego.Function{
			Name:             GetDataBuiltinName,
			Decl:             types.NewFunction(types.Args(types.S, types.S), types.A),
			Memoize:          true,
			Nondeterministic: true,
		},
		f.getDataBuiltin,
	)
}

func (f *PluginFactory) Store() storage.Store {
	// Return the original store since we're injecting data into it
	return f.originalStore
}

// Validate validates the plugin configuration.
func (f *PluginFactory) Validate(manager *plugins.Manager, config []byte) (any, error) {
	logger := manager.Logger()
	if logger == nil {
		logger = logging.New() // Create a default logger for testing scenarios
	}
	f.logger = logger

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

	// Store reference to original store
	f.originalStore = manager.Store

	// Create the bucket data manager (new architecture)
	bucketDataManager, err := NewBucketDataManager(pluginConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket data manager: %w", err)
	}

	f.bucketDataManager = bucketDataManager
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
	if config.MaxBucketsWatchers == 0 {
		config.MaxBucketsWatchers = 10
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

	plugin := &Plugin{
		manager:           manager,
		config:            pluginConfig,
		bucketDataManager: f.bucketDataManager,
		logger:            logger,
	}

	return plugin
}

// Plugin represents the NATS K/V cache plugin instance.
type Plugin struct {
	manager           *plugins.Manager
	config            *Config
	bucketDataManager *BucketDataManager
	logger            logging.Logger
}

// Start initializes and starts the plugin.
func (p *Plugin) Start(ctx context.Context) error {
	p.logger.Info("Starting NATS K/V data injection plugin")

	if err := p.bucketDataManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start bucket data manager: %w", err)
	}

	// If we have a root bucket configured, load it immediately
	if p.config.RootBucket != "" {
		p.logger.Info("Loading root bucket: %s", p.config.RootBucket)
		if err := p.bucketDataManager.EnsureBucketLoaded(ctx, p.config.RootBucket, p.manager.Store, true); err != nil {
			return err
		}
	}

	p.logger.Info("NATS plugin started successfully")
	return nil
}

// Stop shuts down the plugin.
func (p *Plugin) Stop(ctx context.Context) {
	p.logger.Info("Stopping NATS K/V data injection plugin")

	if err := p.bucketDataManager.Stop(ctx); err != nil {
		p.logger.Error("Error stopping bucket data manager: %v", err)
	}

	p.logger.Info("NATS plugin stopped")
}

// Reconfigure updates the plugin configuration.
func (p *Plugin) Reconfigure(ctx context.Context, config any) {
	p.logger.Info("Reconfiguring NATS K/V data injection plugin")

	newConfig, ok := config.(*Config)
	if !ok {
		p.logger.Error("Invalid config type for reconfiguration")
		return
	}

	// Stop the current manager
	if err := p.bucketDataManager.Stop(ctx); err != nil {
		p.logger.Error("Error stopping bucket data manager during reconfiguration: %v", err)
	}

	// Update configuration
	p.config = newConfig

	// Create new bucket data manager with updated config
	bucketDataManager, err := NewBucketDataManager(newConfig, p.logger)
	if err != nil {
		p.logger.Error("Failed to create new bucket data manager during reconfiguration: %v", err)
		return
	}

	if err := bucketDataManager.Start(ctx); err != nil {
		p.logger.Error("Failed to start new bucket data manager during reconfiguration: %v", err)
		return
	}

	p.bucketDataManager = bucketDataManager

	p.logger.Info("NATS K/V data injection plugin reconfigured successfully")
}
