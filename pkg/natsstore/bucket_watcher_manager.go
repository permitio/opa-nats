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
//
// Stop/watchLoop handshake:
//   - stopReq is closed by Stop to broadcast "please exit" to watchLoop.
//     Closing (rather than sending) is what makes the handshake race-free —
//     a closed channel is observable by every receiver, whereas a single
//     value sent on a buffered channel can be drained by either side first.
//   - watcherLoopExited is closed by watchLoop in a defer once the loop
//     returns, signaling Stop that the goroutine has actually exited.
type BucketWatcher struct {
	bucketName        string
	watcher           nats.KeyWatcher
	natsClient        *NATSClient
	dataTransformer   *DataTransformer
	opaStore          storage.Store // Reference to OPA store for data injection
	logger            logging.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.RWMutex
	started           bool
	stopReq           chan struct{}
	watcherLoopExited chan struct{}
	isRoot            bool
}

// NewBucketWatcher creates a new bucket-specific watcher.
func NewBucketWatcher(bucketName string, natsClient *NATSClient, logger logging.Logger, dataTransformer *DataTransformer, opaStore storage.Store, isRoot bool) (*BucketWatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	watcher := &BucketWatcher{
		bucketName:        bucketName,
		natsClient:        natsClient,
		dataTransformer:   dataTransformer,
		opaStore:          opaStore,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
		isRoot:            isRoot,
		stopReq:           make(chan struct{}),
		watcherLoopExited: make(chan struct{}),
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

	// Reject unsafe tenant tokens before they become a NATS subject filter.
	if err := validateTenant(gw.bucketName); err != nil {
		return err
	}

	// Open the single muxed bucket (handle is cached).
	kv, err := gw.natsClient.getBucket()
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}
	if err := gw.dataTransformer.LoadBucketDataBulk(gw.ctx, gw.bucketName, gw.natsClient, gw.opaStore, gw.isRoot); err != nil {
		return fmt.Errorf("failed to load data for tenant %s: %w", gw.bucketName, err)
	}

	// Watch ONLY this tenant's slice. gw.bucketName is the tenant token; the
	// single-filter "<tenant>.>" maps to the scopeable extended consumer form
	// (one ordered consumer per watched tenant), never the whole bucket.
	watchPattern := gw.bucketName + ".>"
	watcher, err := kv.Watch(watchPattern, nats.Context(gw.ctx))
	if err != nil {
		return fmt.Errorf("failed to create watcher for tenant %s: %w", gw.bucketName, err)
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
			gw.opaStore.Abort(gw.ctx, txn)
			gw.logger.Error("Aborting clean transaction: %v", err)
		}
		if err := gw.opaStore.Commit(gw.ctx, txn); err != nil {
			gw.logger.Error("Failed to commit clean transaction: %v", err)
		} else {
			gw.logger.Debug("Committed clean transaction")
		}
	}()

	err = gw.opaStore.Write(gw.ctx, txn, storage.RemoveOp, storage.Path{"nats", "kv", gw.bucketName}, nil)
	if err != nil {
		return fmt.Errorf("failed to write to OPA store: %w", err)
	}

	return nil
}

// Stop shuts down the bucket watcher.
//
// The handshake — close(stopReq) → watchLoop returns → defer
// close(watcherLoopExited) → <-watcherLoopExited unblocks — is race-free
// regardless of whether watchLoop has already parked in its select. A
// previous version used a single buffered channel for both directions,
// which deadlocked watchLoop when Stop's send completed before watchLoop
// reached the select (Stop's own subsequent receive drained the value).
func (gw *BucketWatcher) Stop() error {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	if !gw.started {
		return nil
	}

	close(gw.stopReq)
	<-gw.watcherLoopExited

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
		close(gw.watcherLoopExited)
		gw.logger.Debug("Watch loop ended for bucket: %s", gw.bucketName)
	}()

	for {
		select {
		case <-gw.stopReq:
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
	path := gw.natsClient.keyToPath(key)

	switch entry.Operation() {
	case nats.KeyValuePut:
		var value any
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
	rootWatcher     *BucketWatcher
	watchers        *lru.Cache[string, *BucketWatcher]
	natsClient      *NATSClient
	dataTransformer *DataTransformer
	logger          logging.Logger
	maxWatchers     int
	// mu protects watchers and stopping. Held briefly for cache reads/writes
	// and for the stopping flag, but NEVER across BucketWatcher.Start (which
	// takes the OPA inmem store's write lock and may block on a parent Rego
	// query's read transaction — see GetOrCreateWatcher for details).
	mu sync.RWMutex
	// createMu serializes the create-and-start path of GetOrCreateWatcher so
	// no two goroutines ever both create+start a watcher for the same bucket.
	// This prevents the duplicate-create race where the loser's Stop() would
	// call cleanOPAStore and wipe the winner's just-loaded data. It also
	// gives BucketWatcherManager.Stop a synchronization point: by acquiring
	// createMu, Stop can be sure no in-flight create can complete after the
	// stopping flag is observed.
	createMu sync.Mutex
	// stopping is true once Stop has been called. New create attempts in
	// GetOrCreateWatcher refuse to proceed when this flag is set, so a
	// watcher cannot be inserted into the cache after the manager has been
	// torn down. Protected by mu.
	stopping bool
	// newWatcher builds and starts a new BucketWatcher. The default wraps
	// NewBucketWatcher + watcher.Start; tests override it to inject failures
	// (e.g. a Start that exercises the OPA store write lock without needing
	// a real NATS connection). Set once in NewBucketWatcherManager; never
	// reassigned at runtime (Plugin.Reconfigure replaces the entire manager
	// rather than mutating it), so no synchronization needed.
	newWatcher func(bucketName string, opaStore storage.Store) (*BucketWatcher, error)
	rootBucket string
}

// NewBucketWatcherManager creates a new bucket watcher manager.
func NewBucketWatcherManager(natsClient *NATSClient, maxWatchers int, logger logging.Logger, config *Config) (*BucketWatcherManager, error) {
	// Create data transformer
	dataTransformer, err := NewDataTransformer(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create data transformer: %w", err)
	}
	manager := &BucketWatcherManager{
		natsClient:      natsClient,
		dataTransformer: dataTransformer,
		logger:          logger,
		maxWatchers:     maxWatchers,
		rootBucket:      config.RootTenant,
	}
	manager.newWatcher = func(bucketName string, opaStore storage.Store) (*BucketWatcher, error) {
		w, err := NewBucketWatcher(bucketName, manager.natsClient, manager.logger, manager.dataTransformer, opaStore, false)
		if err != nil {
			return nil, err
		}
		if err := w.Start(); err != nil {
			return nil, err
		}
		return w, nil
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
	// Plain LRU without an eviction callback — we MUST stop evicted watchers
	// outside gwm.mu, so eviction is performed manually in GetOrCreateWatcher
	// (see the comment on the slow path for the deadlock this avoids).
	cache, err := lru.New[string, *BucketWatcher](gwm.maxWatchers)
	if err != nil {
		return fmt.Errorf("failed to create LRU cache: %w", err)
	}
	gwm.watchers = cache
	return nil
}

// CreateRootWatcher builds and starts the root-bucket watcher. The root
// watcher lives outside the LRU cache (it must never be evicted) but uses
// the same createMu/stopping flow as GetOrCreateWatcher so that:
//   - Stop() drains any in-flight CreateRootWatcher (createMu acts as the
//     synchronization point), and
//   - a CreateRootWatcher started after Stop is refused, preventing a
//     background watch loop from being attached to a torn-down manager.
//
// watcher.Start runs without gwm.mu held, for the same reason described
// on GetOrCreateWatcher: Start ultimately takes the OPA inmem store's
// write lock, and holding gwm.mu across that wait would deadlock with
// any HasWatcher / GetOrCreateWatcher fast-path caller in the parent
// Rego query.
func (gwm *BucketWatcherManager) CreateRootWatcher(opaStore storage.Store) (*BucketWatcher, error) {
	gwm.createMu.Lock()
	defer gwm.createMu.Unlock()

	gwm.mu.RLock()
	stopping := gwm.stopping
	gwm.mu.RUnlock()
	if stopping {
		return nil, fmt.Errorf("bucket watcher manager is stopping, refusing to create root watcher for %s", gwm.rootBucket)
	}

	watcher, err := NewBucketWatcher(gwm.rootBucket, gwm.natsClient, gwm.logger, gwm.dataTransformer, opaStore, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create root bucket watcher: %w", err)
	}
	if err := watcher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start bucket watcher for %s: %w", gwm.rootBucket, err)
	}

	gwm.mu.Lock()
	gwm.rootWatcher = watcher
	gwm.mu.Unlock()
	return watcher, nil
}

// GetOrCreateWatcher gets an existing watcher for a bucket or creates a new one.
//
// Concurrency model and the bug this avoids:
//
// watcher.Start writes to the OPA inmem store via Commit (which takes the
// store's RWMutex.Lock). watchBucketBuiltin invokes this function from a
// goroutine spawned inside a Rego builtin call, while the parent Rego
// query still holds the inmem store's RLock for its read transaction.
// Commit therefore blocks until the parent's read transaction closes.
//
// If we held gwm.mu (write) across Start, the parent's next watch_bucket
// call would hit HasWatcher → gwm.mu.RLock → blocked behind us → deadlock.
//
// We instead:
//  1. Fast-path check the LRU cache under gwm.mu.RLock.
//  2. Take createMu (a separate mutex) to serialize creates so that two
//     callers for the same bucket never both create+Start a watcher. This
//     is necessary because BucketWatcher.Stop calls cleanOPAStore, which
//     RemoveOps everything under /nats/kv/<bucket>; if a duplicate-create
//     race occurred, stopping the loser would wipe the winner's data.
//  3. Re-check the cache (another caller may have created it while we
//     waited on createMu) and the stopping flag.
//  4. NewBucketWatcher + Start, holding only createMu (which Stop also
//     acquires). HasWatcher / GetOrCreateWatcher fast-path callers use
//     gwm.mu.RLock and are NOT blocked by us, so the parent Rego query
//     can finish and release the inmem store's RLock — letting our Commit
//     proceed.
//  5. Insert into the LRU cache under gwm.mu.Lock.
func (gwm *BucketWatcherManager) GetOrCreateWatcher(bucketName string, opaStore storage.Store) (*BucketWatcher, error) {
	// Fast path: check if a watcher already exists under read lock.
	gwm.mu.RLock()
	if watcher, exists := gwm.watchers.Get(bucketName); exists {
		gwm.mu.RUnlock()
		gwm.logger.Debug("Using existing bucket watcher for: %s", bucketName)
		return watcher, nil
	}
	gwm.mu.RUnlock()

	// Slow path: serialize creates with createMu so that no two callers
	// for the same bucket can both create+Start a watcher. createMu is
	// distinct from gwm.mu, so HasWatcher and the fast path are NOT
	// blocked while we are creating.
	gwm.createMu.Lock()
	defer gwm.createMu.Unlock()

	// Refuse new creates after Stop has been called, otherwise a watcher
	// could be inserted into the cache after the manager was torn down,
	// leaving a background watch loop running and writing to the (now
	// shutdown-bound) OPA store.
	gwm.mu.RLock()
	stopping := gwm.stopping
	gwm.mu.RUnlock()
	if stopping {
		return nil, fmt.Errorf("bucket watcher manager is stopping, refusing to create watcher for %s", bucketName)
	}

	// Double-check the cache: another caller may have created the watcher
	// while we were waiting on createMu.
	gwm.mu.RLock()
	if watcher, exists := gwm.watchers.Get(bucketName); exists {
		gwm.mu.RUnlock()
		gwm.logger.Debug("Using existing bucket watcher for %s after createMu wait", bucketName)
		return watcher, nil
	}
	gwm.mu.RUnlock()

	// We are the unique creator for this bucket. Build and start via the
	// newWatcher seam so tests can inject a Start that exercises the OPA
	// store write lock without needing a real NATS connection.
	watcher, err := gwm.newWatcher(bucketName, opaStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket watcher for %s: %w", bucketName, err)
	}

	// Insert into the LRU cache. createMu is held for the entire slow path
	// and Stop() takes createMu BEFORE setting gwm.stopping=true, so
	// gwm.stopping cannot transition false→true here — no late stopping
	// re-check needed. By holding createMu we also know no other creator
	// inserted a watcher for this bucket between the double-check above
	// and here, so we can Add unconditionally — the duplicate-create-
	// Stop()-wipes-data race is impossible.
	//
	// Eviction is performed MANUALLY rather than letting the LRU fire an
	// onEviction callback under gwm.mu.Lock. An eviction callback would
	// call BucketWatcher.Stop → cleanOPAStore → opaStore.Commit, which
	// takes the inmem store's write lock and blocks on the parent Rego
	// query's RLock — exactly the deadlock this PR is meant to fix, just
	// shifted from Start to eviction. We therefore peek capacity and
	// RemoveOldest under gwm.mu, drop the lock, then call Stop on the
	// evicted watcher with no manager lock held.
	var evictedWatcher *BucketWatcher
	gwm.mu.Lock()
	if gwm.watchers.Len() >= gwm.maxWatchers {
		if _, v, ok := gwm.watchers.RemoveOldest(); ok {
			evictedWatcher = v
		}
	}
	// Capacity is now guaranteed available, so this Add never evicts.
	_ = gwm.watchers.Add(bucketName, watcher)
	gwm.mu.Unlock()

	if evictedWatcher != nil {
		gwm.logger.Debug("Added new bucket watcher for %s, evicted LRU watcher", bucketName)
		if stopErr := evictedWatcher.Stop(); stopErr != nil {
			gwm.logger.Error("Failed to stop evicted bucket watcher: %v", stopErr)
		}
	} else {
		gwm.logger.Debug("Added new bucket watcher for: %s", bucketName)
	}
	return watcher, nil
}

// Stop shuts down all bucket watchers. After Stop returns, GetOrCreateWatcher
// will refuse to create new watchers.
//
// Ordering matters: we acquire createMu first to drain any in-flight create
// (which may be blocked on the OPA store's write lock) and to keep new
// creates from slipping past the stopping flag. Only then do we set
// stopping=true and tear down the cached watchers.
func (gwm *BucketWatcherManager) Stop() error {
	// Drain in-flight creates and block new ones for the duration of Stop.
	gwm.createMu.Lock()
	defer gwm.createMu.Unlock()

	gwm.mu.Lock()
	gwm.stopping = true
	keys := gwm.watchers.Keys()
	rootWatcher := gwm.rootWatcher
	gwm.rootWatcher = nil
	gwm.mu.Unlock()

	// Stop the root watcher (if any) outside gwm.mu — its Stop calls
	// cleanOPAStore which takes the OPA store's write lock, the very lock
	// holding-pattern this manager is structured to avoid under gwm.mu.
	if rootWatcher != nil {
		if err := rootWatcher.Stop(); err != nil {
			gwm.logger.Error("Failed to stop root bucket watcher: %v", err)
		}
	}

	for _, bucket := range keys {
		gwm.mu.RLock()
		watcher, exists := gwm.watchers.Get(bucket)
		gwm.mu.RUnlock()
		if !exists {
			continue
		}
		if err := watcher.Stop(); err != nil {
			gwm.logger.Error("Failed to stop bucket watcher %s: %v", bucket, err)
		}
	}

	// Purge clears the cache. Each cached watcher was already stopped above;
	// because the LRU is created without an eviction callback, Purge does
	// not invoke any side effects under the lock — it is just a map clear.
	gwm.mu.Lock()
	gwm.watchers.Purge()
	gwm.mu.Unlock()

	gwm.logger.Info("Stopped all bucket watchers")
	return nil
}
