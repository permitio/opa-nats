package natsstore

import (
	"sync"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestManager builds a BucketWatcherManager with an LRU cache, no NATS
// dependencies, and no eviction stops (so we can drive the cache directly
// from tests without spinning up a real watcher).
func newTestManager(t *testing.T, capacity int) *BucketWatcherManager {
	t.Helper()
	cache, err := lru.New[string, *BucketWatcher](capacity)
	require.NoError(t, err)
	return &BucketWatcherManager{
		watchers:    cache,
		logger:      logging.Get(),
		maxWatchers: capacity,
	}
}

// TestGetOrCreateWatcher_FastPath verifies that a cache hit returns
// the cached watcher without taking createMu (so concurrent fast-path
// callers don't serialize behind a slow create on a different bucket).
func TestGetOrCreateWatcher_FastPath(t *testing.T) {
	m := newTestManager(t, 8)
	cached := &BucketWatcher{bucketName: "preloaded"}
	m.watchers.Add("preloaded", cached)

	got, err := m.GetOrCreateWatcher("preloaded", inmem.New())
	require.NoError(t, err)
	assert.Same(t, cached, got, "fast path must return the cached watcher")
}

// TestGetOrCreateWatcher_RefusedAfterStop verifies that once the manager
// is stopping, GetOrCreateWatcher refuses to create new watchers (so a
// background watch loop cannot be inserted into the cache after the
// manager has been torn down).
func TestGetOrCreateWatcher_RefusedAfterStop(t *testing.T) {
	m := newTestManager(t, 8)

	// Mark the manager as stopping without going through Stop() so we
	// don't depend on the watcher.Stop() side effects in this test.
	m.mu.Lock()
	m.stopping = true
	m.mu.Unlock()

	got, err := m.GetOrCreateWatcher("never-created", inmem.New())
	assert.Nil(t, got)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopping")
}

// TestGetOrCreateWatcher_FastPathAfterStopReturnsCached verifies that
// the fast-path cache hit still works after Stop has been called. The
// stopping flag only blocks new creates, not lookups of watchers that
// are still in the cache (Stop tears them down via the watchers slice).
func TestGetOrCreateWatcher_FastPathAfterStopReturnsCached(t *testing.T) {
	m := newTestManager(t, 8)
	cached := &BucketWatcher{bucketName: "preloaded"}
	m.watchers.Add("preloaded", cached)

	m.mu.Lock()
	m.stopping = true
	m.mu.Unlock()

	got, err := m.GetOrCreateWatcher("preloaded", inmem.New())
	require.NoError(t, err)
	assert.Same(t, cached, got)
}

// TestStop_IsIdempotent verifies Stop can be called multiple times without
// deadlocking on createMu (each call takes and releases it).
func TestStop_IsIdempotent(t *testing.T) {
	m := newTestManager(t, 8)
	require.NoError(t, m.Stop())
	require.NoError(t, m.Stop())
	require.NoError(t, m.Stop())

	// After Stop, new creates are refused.
	_, err := m.GetOrCreateWatcher("anything", inmem.New())
	require.Error(t, err)
}

// TestGetOrCreateWatcher_ConcurrentFastPath verifies that many concurrent
// fast-path callers for the same and different buckets all succeed, with
// no goroutine blocking another. This is a smoke test for the RWMutex
// scope reduction — if we accidentally hold gwm.mu (write) on the fast
// path, this test would still pass but it serves as documentation of the
// expected non-blocking behavior.
func TestGetOrCreateWatcher_ConcurrentFastPath(t *testing.T) {
	m := newTestManager(t, 32)
	for _, name := range []string{"a", "b", "c", "d"} {
		m.watchers.Add(name, &BucketWatcher{bucketName: name})
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		bucket := []string{"a", "b", "c", "d"}[i%4]
		go func() {
			defer wg.Done()
			_, err := m.GetOrCreateWatcher(bucket, inmem.New())
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error from fast-path call: %v", err)
	}
}
