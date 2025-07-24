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
)

// GroupWatcher manages watching and caching for a specific group.
type GroupWatcher struct {
	group      string
	cache      *GroupCache
	watcher    nats.KeyWatcher
	natsClient *NATSClient
	logger     logging.Logger
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
	started    bool
	lastAccess time.Time
}

// NewGroupWatcher creates a new group-specific watcher.
func NewGroupWatcher(group string, natsClient *NATSClient, cacheSize int, logger logging.Logger) (*GroupWatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create cache for this group - cache ALL keys under this group
	cache, err := NewGroupCache(cacheSize, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create cache for group %s: %w", group, err)
	}

	watcher := &GroupWatcher{
		group:      group,
		cache:      cache,
		natsClient: natsClient,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		lastAccess: time.Now(),
	}

	return watcher, nil
}

// Start begins watching for changes to this group.
func (gw *GroupWatcher) Start() error {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	if gw.started {
		return nil
	}

	// Determine which bucket this group should use
	// Create a sample path to extract the bucket name
	samplePath := []string{"groups", gw.group, "metadata"}
	bucketName := gw.natsClient.determineBucket(samplePath)

	// Get or create the bucket for this group
	kv, err := gw.natsClient.getOrCreateBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket %s for group %s: %w", bucketName, gw.group, err)
	}

	// Create watcher for all keys in this group's bucket
	// Since the bucket IS the group, watch everything in the bucket
	watchPattern := ">" // Watch all keys in this bucket
	watcher, err := kv.Watch(watchPattern, nats.Context(gw.ctx))
	if err != nil {
		return fmt.Errorf("failed to create watcher for group %s (bucket %s): %w", gw.group, bucketName, err)
	}

	gw.watcher = watcher
	gw.started = true

	// Start watching in background
	go gw.watchLoop()

	// Load existing data for this group into cache
	go gw.loadExistingDataFromBucket(kv)

	gw.logger.Debug("Started group watcher for group %s using bucket: %s", gw.group, bucketName)
	return nil
}

// Stop shuts down the group watcher.
func (gw *GroupWatcher) Stop() error {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	if !gw.started {
		return nil
	}

	gw.cancel()

	if gw.watcher != nil {
		if err := gw.watcher.Stop(); err != nil {
			gw.logger.Error("Failed to stop watcher for group %s: %v", gw.group, err)
		}
	}

	gw.started = false
	gw.logger.Debug("Stopped group watcher for: %s", gw.group)
	return nil
}

// stripGroupPrefix removes the group prefix from a path to get the cache key.
func (gw *GroupWatcher) stripGroupPrefix(path []string) []string {
	// Find where the group appears in the path and take everything after it
	for i, segment := range path {
		if segment == gw.group {
			// Take everything after the group segment
			if i+1 < len(path) {
				return path[i+1:]
			}
			// If nothing after group, return empty path
			return []string{}
		}
	}
	// If group not found, return original path (shouldn't happen in group mode)
	return path
}

// Get retrieves a value from this group's cache or NATS K/V.
func (gw *GroupWatcher) Get(path []string) (interface{}, bool, error) {
	gw.updateLastAccess()

	// Strip group prefix to get cache key
	cacheKey := gw.stripGroupPrefix(path)

	// Try cache first using stripped key
	if value, found := gw.cache.Get(cacheKey); found {
		gw.logger.Debug("Group cache hit for group %s, cache key: %v, original path: %v", gw.group, cacheKey, path)
		return value, true, nil
	}

	// Cache miss - fetch from NATS K/V and cache it
	value, err := gw.natsClient.Get(path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get from NATS for group %s: %w", gw.group, err)
	}

	if value != nil {
		// Store in cache using stripped key
		gw.cache.Put(cacheKey, value)
		gw.logger.Debug("Group cache miss, fetched and cached for group %s, cache key: %v, original path: %v", gw.group, cacheKey, path)
		return value, true, nil
	}

	return nil, false, nil
}

// Put stores a value in both NATS K/V and this group's cache.
func (gw *GroupWatcher) Put(path []string, value interface{}) error {
	gw.updateLastAccess()

	// Store in NATS K/V
	if err := gw.natsClient.Put(path, value); err != nil {
		return fmt.Errorf("failed to put to NATS for group %s: %w", gw.group, err)
	}

	// Strip group prefix for cache key
	cacheKey := gw.stripGroupPrefix(path)

	// Update cache using stripped key
	gw.cache.Put(cacheKey, value)

	gw.logger.Debug("Stored value for group %s, cache key: %v, original path: %v", gw.group, cacheKey, path)
	return nil
}

// Delete removes a value from both NATS K/V and this group's cache.
func (gw *GroupWatcher) Delete(path []string) error {
	gw.updateLastAccess()

	// Delete from NATS K/V
	if err := gw.natsClient.Delete(path); err != nil {
		return fmt.Errorf("failed to delete from NATS for group %s: %w", gw.group, err)
	}

	// Strip group prefix for cache key
	cacheKey := gw.stripGroupPrefix(path)

	// Remove from cache using stripped key
	gw.cache.Remove(cacheKey)

	gw.logger.Debug("Deleted value for group %s, cache key: %v, original path: %v", gw.group, cacheKey, path)
	return nil
}

// updateLastAccess updates the last access time for LRU tracking.
func (gw *GroupWatcher) updateLastAccess() {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	gw.lastAccess = time.Now()
}

// GetLastAccess returns the last access time.
func (gw *GroupWatcher) GetLastAccess() time.Time {
	gw.mu.RLock()
	defer gw.mu.RUnlock()
	return gw.lastAccess
}

// GetStats returns statistics for this group watcher.
func (gw *GroupWatcher) GetStats() map[string]interface{} {
	gw.mu.RLock()
	defer gw.mu.RUnlock()

	stats := gw.cache.GetStats()
	stats["group"] = gw.group
	stats["started"] = gw.started
	stats["last_access"] = gw.lastAccess.Format(time.RFC3339)

	return stats
}

// loadExistingDataFromBucket loads all existing data for this group from the specified NATS K/V bucket into the cache.
func (gw *GroupWatcher) loadExistingDataFromBucket(kv nats.KeyValue) {
	defer func() {
		if r := recover(); r != nil {
			gw.logger.Error("Panic in loadExistingDataFromBucket for group %s: %v", gw.group, r)
		}
	}()

	// Since each group has its own bucket, watch all keys in this bucket
	watchPattern := ">" // Watch all keys in the bucket

	// Use Watch to get all existing entries from the group's bucket
	tempWatcher, err := kv.Watch(watchPattern, nats.Context(gw.ctx))
	if err != nil {
		gw.logger.Error("Failed to create temp watcher for loading data for group %s: %v", gw.group, err)
		return
	}
	defer func() {
		if err := tempWatcher.Stop(); err != nil {
			gw.logger.Error("Failed to stop temp watcher for group %s: %v", gw.group, err)
		}
	}()

	// Process initial state and then stop
	timeout := time.After(5 * time.Second) // Timeout for initial load

	for {
		select {
		case <-gw.ctx.Done():
			return
		case <-timeout:
			gw.logger.Debug("Timeout loading existing data for group %s", gw.group)
			return
		case entry := <-tempWatcher.Updates():
			if entry == nil {
				continue
			}

			// Only load PUT operations (existing data)
			if entry.Operation() == nats.KeyValuePut {
				gw.handleKVUpdate(entry)
			}
		}
	}
}

// watchLoop handles incoming changes for this group.
func (gw *GroupWatcher) watchLoop() {
	defer func() {
		gw.logger.Debug("Watch loop ended for group: %s", gw.group)
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

// handleKVUpdate processes a K/V update for this group.
func (gw *GroupWatcher) handleKVUpdate(entry nats.KeyValueEntry) {
	key := entry.Key()
	path, err := gw.natsClient.keyToPath(key)
	if err != nil {
		gw.logger.Error("Failed to parse key %s for group %s: %v", key, gw.group, err)
		return
	}

	switch entry.Operation() {
	case nats.KeyValuePut:
		var value interface{}
		if err := json.Unmarshal(entry.Value(), &value); err != nil {
			// If JSON unmarshal fails, store as string
			value = string(entry.Value())
		}

		gw.cache.Put(path, value)
		gw.logger.Debug("Updated group cache for group %s, path: %v", gw.group, path)

	case nats.KeyValueDelete, nats.KeyValuePurge:
		gw.cache.Remove(path)
		gw.logger.Debug("Removed from group cache for group %s, path: %v", gw.group, path)
	}
}

// GroupWatcherManager manages multiple group watchers with LRU eviction.
type GroupWatcherManager struct {
	watchers         *lru.Cache[string, *GroupWatcher]
	natsClient       *NATSClient
	logger           logging.Logger
	maxWatchers      int
	watcherCacheSize int
	watcherTTL       time.Duration
	mu               sync.RWMutex
	groupRegex       []*regexp.Regexp

	// Single group mode
	singleGroupMode bool
	singleGroup     string
}

// NewGroupWatcherManager creates a new group watcher manager.
func NewGroupWatcherManager(natsClient *NATSClient, maxWatchers, watcherCacheSize int, logger logging.Logger, config *Config) (*GroupWatcherManager, error) {
	// Create LRU cache for watchers with eviction callback
	cache, err := lru.NewWithEvict[string, *GroupWatcher](maxWatchers, func(key string, value *GroupWatcher) {
		// Stop evicted watcher
		if err := value.Stop(); err != nil {
			logger.Error("Failed to stop evicted group watcher for %s: %v", key, err)
		}
		logger.Debug("Evicted group watcher for: %s", key)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	// Determine mode: single group or pattern-based
	singleGroupMode := config.SingleGroup != ""
	var groupRegex *regexp.Regexp

	if !singleGroupMode && config.GroupRegexPattern != "" {
		// Compile the group regex pattern
		groupRegex, err = regexp.Compile(config.GroupRegexPattern)
		if err != nil {
			return nil, fmt.Errorf("invalid group regex pattern '%s': %w", config.GroupRegexPattern, err)
		}
	}

	return &GroupWatcherManager{
		watchers:         cache,
		natsClient:       natsClient,
		logger:           logger,
		maxWatchers:      maxWatchers,
		watcherCacheSize: watcherCacheSize,
		groupRegex:       []*regexp.Regexp{groupRegex}, // Keep as slice for compatibility
		singleGroupMode:  singleGroupMode,
		singleGroup:      config.SingleGroup,
	}, nil
}

// ExtractGroup extracts the group from a path using the configured pattern or single group mode.
func (gwm *GroupWatcherManager) ExtractGroup(path []string) (string, bool) {
	if gwm.singleGroupMode {
		// Single group mode - always use the configured group
		return gwm.singleGroup, true
	}

	// Pattern-based mode
	if len(gwm.groupRegex) == 0 || gwm.groupRegex[0] == nil {
		return "", false
	}

	pathStr := strings.Join(path, "/")
	matches := gwm.groupRegex[0].FindStringSubmatch(pathStr)
	if len(matches) > 1 {
		// Return the first capture group which should be the group identifier
		return matches[1], true
	}

	return "", false
}

// GetOrCreateWatcher gets an existing watcher for a group or creates a new one.
func (gwm *GroupWatcherManager) GetOrCreateWatcher(group string) (*GroupWatcher, error) {
	gwm.mu.Lock()
	defer gwm.mu.Unlock()

	// Try to get existing watcher
	if watcher, exists := gwm.watchers.Get(group); exists {
		gwm.logger.Debug("Using existing group watcher for: %s", group)
		return watcher, nil
	}

	// Create new watcher
	watcher, err := NewGroupWatcher(group, gwm.natsClient, gwm.watcherCacheSize, gwm.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create group watcher for %s: %w", group, err)
	}

	// Start the watcher
	if err := watcher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start group watcher for %s: %w", group, err)
	}

	// Add to LRU cache (this may evict the least recently used watcher)
	evicted := gwm.watchers.Add(group, watcher)
	if evicted {
		gwm.logger.Debug("Added new group watcher for %s, evicted LRU watcher", group)
	} else {
		gwm.logger.Debug("Added new group watcher for: %s", group)
	}

	return watcher, nil
}

// Get retrieves a value using group-based caching.
func (gwm *GroupWatcherManager) Get(ctx context.Context, path []string) (interface{}, bool, error) {
	// Extract group from path
	group, hasGroup := gwm.ExtractGroup(path)
	if !hasGroup {
		return nil, false, fmt.Errorf("no group found in path: %v", path)
	}

	// Get or create watcher for this group
	watcher, err := gwm.GetOrCreateWatcher(group)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get watcher for group %s: %w", group, err)
	}

	// Get value from group-specific watcher
	return watcher.Get(path)
}

// Set stores a value using group-based caching.
func (gwm *GroupWatcherManager) Set(ctx context.Context, path []string, value interface{}) error {
	// Extract group from path
	group, hasGroup := gwm.ExtractGroup(path)
	if !hasGroup {
		return fmt.Errorf("no group found in path: %v", path)
	}

	// Get or create watcher for this group
	watcher, err := gwm.GetOrCreateWatcher(group)
	if err != nil {
		return fmt.Errorf("failed to get watcher for group %s: %w", group, err)
	}

	// Set value using group-specific watcher
	return watcher.Put(path, value)
}

// Delete removes a value using group-based caching.
func (gwm *GroupWatcherManager) Delete(ctx context.Context, path []string) error {
	// Extract group from path
	group, hasGroup := gwm.ExtractGroup(path)
	if !hasGroup {
		return fmt.Errorf("no group found in path: %v", path)
	}

	// Get or create watcher for this group
	watcher, err := gwm.GetOrCreateWatcher(group)
	if err != nil {
		return fmt.Errorf("failed to get watcher for group %s: %w", group, err)
	}

	// Delete value using group-specific watcher
	return watcher.Delete(path)
}

// Stop shuts down all group watchers.
func (gwm *GroupWatcherManager) Stop() error {
	gwm.mu.Lock()
	defer gwm.mu.Unlock()

	for _, group := range gwm.watchers.Keys() {
		if watcher, exists := gwm.watchers.Get(group); exists {
			if err := watcher.Stop(); err != nil {
				gwm.logger.Error("Failed to stop group watcher %s: %v", group, err)
			}
		}
	}

	gwm.watchers.Purge()
	gwm.logger.Info("Stopped all group watchers")
	return nil
}

// GetStats returns statistics for all group watchers.
func (gwm *GroupWatcherManager) GetStats() map[string]interface{} {
	gwm.mu.RLock()
	defer gwm.mu.RUnlock()

	stats := map[string]interface{}{
		"active_watchers":    gwm.watchers.Len(),
		"max_watchers":       gwm.maxWatchers,
		"watcher_cache_size": gwm.watcherCacheSize,
		"watcher_ttl":        gwm.watcherTTL.String(),
		"single_group_mode":  gwm.singleGroupMode,
	}

	if gwm.singleGroupMode {
		if gwm.singleGroup != "" {
			stats["single_group"] = gwm.singleGroup
		}
		if gwm.groupRegex != nil && len(gwm.groupRegex) > 0 && gwm.groupRegex[0] != nil {
			stats["single_group_pattern"] = gwm.groupRegex[0].String()
		}
	} else {
		stats["group_patterns_count"] = len(gwm.groupRegex)
	}

	// Add individual watcher stats
	watcherStats := make(map[string]interface{})
	for _, group := range gwm.watchers.Keys() {
		if watcher, exists := gwm.watchers.Get(group); exists {
			watcherStats[group] = watcher.GetStats()
		}
	}
	stats["watchers"] = watcherStats

	return stats
}
