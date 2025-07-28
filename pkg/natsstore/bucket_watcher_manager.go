package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
)

// BucketWatcher manages watching and caching for a specific bucket.
type BucketWatcher struct {
	bucketName      string
	watcher         nats.KeyWatcher
	natsClient      *NATSClient
	dataTransformer *DataTransformer
	opaStore        storage.Store // Reference to OPA store for data injection
	logger          logging.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.RWMutex
	started         bool
	lastAccess      time.Time
}

// NewBucketWatcher creates a new bucket-specific watcher.
func NewBucketWatcher(bucketName string, natsClient *NATSClient, logger logging.Logger, dataTransformer *DataTransformer, opaStore storage.Store) (*BucketWatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	watcher := &BucketWatcher{
		bucketName:      bucketName,
		natsClient:      natsClient,
		dataTransformer: dataTransformer,
		opaStore:        opaStore,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		lastAccess:      time.Now(),
	}

	return watcher, nil
}

// Start begins watching for changes to this bucket.
func (gw *BucketWatcher) Start() error {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	if gw.started {
		return nil
	}

	// Get or create the bucket for this bucket
	kv, err := gw.natsClient.getBucket(gw.bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket %s: %w", gw.bucketName, err)
	}
	if err := gw.dataTransformer.LoadBucketDataBulk(gw.ctx, gw.bucketName, gw.natsClient, gw.opaStore); err != nil {
		return fmt.Errorf("failed to load bucket data for %s: %w", gw.bucketName, err)
	}

	// Create watcher for all keys in this bucket
	watchPattern := ">" // Watch all keys in this bucket
	watcher, err := kv.Watch(watchPattern, nats.Context(gw.ctx))
	if err != nil {
		return fmt.Errorf("failed to create watcher for bucket %s: %w", gw.bucketName, err)
	}

	gw.watcher = watcher
	gw.started = true

	// Start watching in background
	go gw.watchLoop()

	gw.logger.Debug("Started bucket watcher for bucket %s", gw.bucketName)
	return nil
}

// Stop shuts down the bucket watcher.
func (gw *BucketWatcher) Stop() error {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	if !gw.started {
		return nil
	}

	gw.cancel()

	if gw.watcher != nil {
		if err := gw.watcher.Stop(); err != nil {
			gw.logger.Error("Failed to stop watcher for bucket %s: %v", gw.bucketName, err)
		}
	}

	gw.started = false
	gw.logger.Debug("Stopped bucket watcher for: %s", gw.bucketName)
	return nil
}

// watchLoop handles incoming changes for this bucket.
func (gw *BucketWatcher) watchLoop() {
	defer func() {
		gw.logger.Debug("Watch loop ended for bucket: %s", gw.bucketName)
	}()

	for {
		select {
		case <-gw.ctx.Done():
			return
		case entry := <-gw.watcher.Updates():
			if entry == nil {
				continue
			}

			gw.handleKVUpdate(entry)
		}
	}
}

// handleKVUpdate processes a K/V update for this bucket.
func (gw *BucketWatcher) handleKVUpdate(entry nats.KeyValueEntry) {
	key := entry.Key()
	path, err := gw.natsClient.keyToPath(key)
	if err != nil {
		gw.logger.Error("Failed to parse key %s for bucket %s: %v", key, gw.bucketName, err)
		return
	}

	switch entry.Operation() {
	case nats.KeyValuePut:
		var value interface{}
		if err := json.Unmarshal(entry.Value(), &value); err != nil {
			// If JSON unmarshal fails, store as string
			value = string(entry.Value())
		}

		// Inject into OPA store
		if err := gw.dataTransformer.InjectDataToOPAStore(gw.ctx, gw.opaStore, gw.bucketName, key, value); err != nil {
			gw.logger.Error("Failed to inject data to OPA store for bucket %s, key %s: %v", gw.bucketName, key, err)
		} else {
			gw.logger.Debug("Injected update to OPA store for bucket %s, path: %v", gw.bucketName, path)
		}

	case nats.KeyValueDelete, nats.KeyValuePurge:
		// Remove from OPA store
		if err := gw.dataTransformer.InjectDataToOPAStore(gw.ctx, gw.opaStore, gw.bucketName, key, nil); err != nil {
			gw.logger.Error("Failed to remove data from OPA store for bucket %s, key %s: %v", gw.bucketName, key, err)
		} else {
			gw.logger.Debug("Removed from OPA store for bucket %s, path: %v", gw.bucketName, path)
		}
	}
}

// BucketWatcherManager manages multiple bucket watchers with LRU eviction.
type BucketWatcherManager struct {
	watchers         *lru.Cache[string, *BucketWatcher]
	natsClient       *NATSClient
	dataTransformer  *DataTransformer
	logger           logging.Logger
	maxWatchers      int
	watcherCacheSize int
	watcherTTL       time.Duration
	mu               sync.RWMutex
	bucketRegex      []*regexp.Regexp
	singleBucketMode bool
	rootBucket       string
}

// NewBucketWatcherManager creates a new bucket watcher manager.
func NewBucketWatcherManager(natsClient *NATSClient, maxWatchers int, logger logging.Logger, config *Config) (*BucketWatcherManager, error) {
	// Create LRU cache for watchers with eviction callback
	cache, err := lru.NewWithEvict[string, *BucketWatcher](maxWatchers, func(key string, value *BucketWatcher) {
		// Stop evicted watcher
		if err := value.Stop(); err != nil {
			logger.Error("Failed to stop evicted bucket watcher for %s: %v", key, err)
		}
		logger.Debug("Evicted bucket watcher for: %s", key)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	// Create data transformer
	dataTransformer, err := NewDataTransformer(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create data transformer: %w", err)
	}

	return &BucketWatcherManager{
		watchers:        cache,
		natsClient:      natsClient,
		dataTransformer: dataTransformer,
		logger:          logger,
		maxWatchers:     maxWatchers,
		rootBucket:      config.RootBucket,
	}, nil
}

// ExtractGroup extracts the bucket from a path using the configured pattern or single bucket mode.
func (gwm *BucketWatcherManager) ExtractBucket(path []string) (string, bool) {
	if gwm.singleBucketMode {
		// Single bucket mode - always use the configured bucket
		return gwm.rootBucket, true
	}

	// Pattern-based mode
	if len(gwm.bucketRegex) == 0 || gwm.bucketRegex[0] == nil {
		return "", false
	}

	pathStr := strings.Join(path, "/")
	matches := gwm.bucketRegex[0].FindStringSubmatch(pathStr)
	if len(matches) > 1 {
		// Return the first capture bucket which should be the bucket identifier
		return matches[1], true
	}

	return "", false
}

func (gwm *BucketWatcherManager) HasWatcher(bucketName string) bool {
	gwm.mu.RLock()
	defer gwm.mu.RUnlock()
	_, isWatched := gwm.watchers.Get(bucketName)

	return isWatched
}

// GetOrCreateWatcher gets an existing watcher for a bucket or creates a new one.
func (gwm *BucketWatcherManager) GetOrCreateWatcher(bucketName string, opaStore storage.Store) (*BucketWatcher, error) {
	gwm.mu.Lock()
	defer gwm.mu.Unlock()

	// Try to get existing watcher
	if watcher, exists := gwm.watchers.Get(bucketName); exists {
		gwm.logger.Debug("Using existing bucket watcher for: %s", bucketName)
		return watcher, nil
	}

	// Create new watcher
	watcher, err := NewBucketWatcher(bucketName, gwm.natsClient, gwm.logger, gwm.dataTransformer, opaStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket watcher for %s: %w", bucketName, err)
	}

	// Start the watcher
	if err := watcher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start bucket watcher for %s: %w", bucketName, err)
	}

	// Add to LRU cache (this may evict the least recently used watcher)
	alreadyExisted, evicted := gwm.watchers.ContainsOrAdd(bucketName, watcher)
	if evicted {
		gwm.logger.Debug("Added new bucket watcher for %s, evicted LRU watcher", bucketName)
	} else if alreadyExisted {
		gwm.logger.Debug("Bucket watcher for %s already existed, stopping the newly created to prevent residues goroutines", bucketName)
		if err := watcher.Stop(); err != nil {
			gwm.logger.Error("Failed to stop bucket watcher for %s: %v", bucketName, err)
		}
	} else {
		gwm.logger.Debug("Added new bucket watcher for: %s", bucketName)
	}
	

	return watcher, nil
}

// Get retrieves a value using bucket-based caching.
func (gwm *BucketWatcherManager) Get(ctx context.Context, path []string) (interface{}, bool, error) {
	// Extract bucket from path
	_, hasBucket := gwm.ExtractBucket(path)
	if !hasBucket {
		return nil, false, fmt.Errorf("no bucket found in path: %v", path)
	}

	// Note: This Get method is deprecated in the new architecture
	// Group data should be loaded via EnsureGroupLoaded instead
	return nil, false, fmt.Errorf("Group.Get method is deprecated - use EnsureGroupLoaded to load data into OPA store")
}

// Set stores a value using bucket-based caching.
func (gwm *BucketWatcherManager) Set(ctx context.Context, path []string, value interface{}) error {
	// Extract bucket from path
	_, hasBucket := gwm.ExtractBucket(path)
	if !hasBucket {
		return fmt.Errorf("no bucket found in path: %v", path)
	}

	// Note: This Set method is deprecated in the new architecture
	// Data should be written directly to NATS, which will trigger watchers to update OPA store
	return fmt.Errorf("Group.Set method is deprecated - write to NATS directly")
}

// Delete removes a value using bucket-based caching.
func (gwm *BucketWatcherManager) Delete(ctx context.Context, path []string) error {
	// Extract bucket from path
	_, hasBucket := gwm.ExtractBucket(path)
	if !hasBucket {
		return fmt.Errorf("no bucket found in path: %v", path)
	}

	// Note: This Delete method is deprecated in the new architecture
	// Data should be deleted directly from NATS, which will trigger watchers to update OPA store
	return fmt.Errorf("Group.Delete method is deprecated - delete from NATS directly")
}

// Stop shuts down all bucket watchers.
func (gwm *BucketWatcherManager) Stop() error {
	gwm.mu.Lock()
	defer gwm.mu.Unlock()

	for _, bucket := range gwm.watchers.Keys() {
		if watcher, exists := gwm.watchers.Get(bucket); exists {
			if err := watcher.Stop(); err != nil {
				gwm.logger.Error("Failed to stop bucket watcher %s: %v", bucket, err)
			}
		}
	}

	gwm.watchers.Purge()
	gwm.logger.Info("Stopped all bucket watchers")
	return nil
}
