package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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
	stopFinished    chan struct{}
	isRoot          bool
}

// NewBucketWatcher creates a new bucket-specific watcher.
func NewBucketWatcher(bucketName string, natsClient *NATSClient, logger logging.Logger, dataTransformer *DataTransformer, opaStore storage.Store, isRoot bool) (*BucketWatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	watcher := &BucketWatcher{
		bucketName:      bucketName,
		natsClient:      natsClient,
		dataTransformer: dataTransformer,
		opaStore:        opaStore,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		isRoot:          isRoot,
		stopFinished:    make(chan struct{}),
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
	if err := gw.dataTransformer.LoadBucketDataBulk(gw.ctx, gw.bucketName, gw.natsClient, gw.opaStore, gw.isRoot); err != nil {
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

func (gw *BucketWatcher) cleanOPAStore() error {
	txn, err := gw.opaStore.NewTransaction(gw.ctx, storage.TransactionParams{
			BasePaths: []string{"nats"},
			Context:   storage.NewContext(),
			Write:     true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer func() {
		if err != nil {
			gw.opaStore.Abort(gw.ctx,txn)
			gw.logger.Error("Aborting clean transaction: %v", err)
		}
		if err := gw.opaStore.Commit(gw.ctx, txn); err != nil {
			gw.logger.Error("Failed to commit clean transaction: %v", err)
		} else {
			gw.logger.Debug("Committed clean transaction")
		}
	}()
	
	err = gw.opaStore.Write(gw.ctx, txn, storage.RemoveOp, storage.Path{"nats","kv",gw.bucketName}, nil)
	if err != nil {
		return fmt.Errorf("failed to write to OPA store: %w", err)
	}
	
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

	// wait for the goroutine to finish
	<-gw.stopFinished
	if gw.watcher != nil {
		if err := gw.watcher.Stop(); err != nil {
			gw.logger.Error("Failed to stop watcher for bucket %s: %v", gw.bucketName, err)
		}
	}
	gw.started = false
	gw.logger.Debug("Stopped bucket watcher for: %s", gw.bucketName)
	if err := gw.cleanOPAStore(); err != nil {
		gw.logger.Warn("Failed to clean OPA store for bucket %s: %v", gw.bucketName, err)
		// we don't consider this as failing to stop the watcher
	}
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
			close(gw.stopFinished)
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
		if err := gw.dataTransformer.InjectDataToOPAStore(gw.ctx, gw.opaStore, gw.bucketName, key, value, gw.isRoot); err != nil {
			gw.logger.Error("Failed to inject data to OPA store for bucket %s, key %s: %v", gw.bucketName, key, err)
		} else {
			gw.logger.Debug("Injected update to OPA store for bucket %s, path: %v", gw.bucketName, path)
		}

	case nats.KeyValueDelete, nats.KeyValuePurge:
		// Remove from OPA store
		if err := gw.dataTransformer.InjectDataToOPAStore(gw.ctx, gw.opaStore, gw.bucketName, key, nil, gw.isRoot); err != nil {
			gw.logger.Error("Failed to remove data from OPA store for bucket %s, key %s: %v", gw.bucketName, key, err)
		} else {
			gw.logger.Debug("Removed from OPA store for bucket %s, path: %v", gw.bucketName, path)
		}
	}
}

// BucketWatcherManager manages multiple bucket watchers with LRU eviction.
type BucketWatcherManager struct {
	rootWatcher      *BucketWatcher
	watchers         *lru.Cache[string, *BucketWatcher]
	natsClient       *NATSClient
	dataTransformer  *DataTransformer
	logger           logging.Logger
	maxWatchers      int
	mu               sync.RWMutex
	rootBucket       string
}

// NewBucketWatcherManager creates a new bucket watcher manager.
func NewBucketWatcherManager(natsClient *NATSClient, maxWatchers int, logger logging.Logger, config *Config) (*BucketWatcherManager, error) {
	// Create data transformer
	dataTransformer, err := NewDataTransformer(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create data transformer: %w", err)
	}
	manager := &BucketWatcherManager{		
		natsClient:      natsClient,
		dataTransformer: dataTransformer,
		logger:          logger,
		maxWatchers:     maxWatchers,
		rootBucket:      config.RootBucket,
	}
	if err := manager.withCache(); err != nil {
		return nil, fmt.Errorf("failed to create watcher cache: %w", err)
	}

	return manager, nil
}

func (gwm *BucketWatcherManager) HasWatcher(bucketName string) bool {
	gwm.mu.RLock()
	defer gwm.mu.RUnlock()
	_, isWatched := gwm.watchers.Get(bucketName)

	return isWatched
}

func (gwm *BucketWatcherManager) withCache() error {	
	// Create LRU cache for watchers with eviction callback
	cache, err := lru.NewWithEvict[string, *BucketWatcher](gwm.maxWatchers, gwm.onEviction)
	if err != nil {
		return fmt.Errorf("failed to create LRU cache: %w", err)
	}
	gwm.watchers = cache
	return nil
}


func (gwm *BucketWatcherManager) onEviction(key string, value *BucketWatcher) {
	if err := value.Stop(); err != nil {
		gwm.logger.Error("Failed to stop evicted bucket watcher for %s: %v", key, err)
	}
	gwm.logger.Debug("Evicted bucket watcher for: %s", key)
}

func (gwm *BucketWatcherManager) CreateRootWatcher(opaStore storage.Store) (*BucketWatcher, error) {
	watcher, err := NewBucketWatcher(gwm.rootBucket, gwm.natsClient, gwm.logger, gwm.dataTransformer, opaStore, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create root bucket watcher: %w", err)
	}
	gwm.rootWatcher = watcher
	if err := watcher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start bucket watcher for %s: %w", gwm.rootBucket, err)
	}
	return watcher, nil
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
	watcher, err := NewBucketWatcher(bucketName, gwm.natsClient, gwm.logger, gwm.dataTransformer, opaStore, false)
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
