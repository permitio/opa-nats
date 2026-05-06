package natsstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
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
	m := &BucketWatcherManager{
		watchers:    cache,
		logger:      logging.Get(),
		maxWatchers: capacity,
	}
	// Default newWatcher returns a started=false stub so tests that exercise
	// the slow path do not need NATS. Tests that need to exercise the OPA
	// store write lock (e.g. the deadlock regression test) override this.
	m.newWatcher = func(bucketName string, _ storage.Store) (*BucketWatcher, error) {
		return &BucketWatcher{bucketName: bucketName}, nil
	}
	return m
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

// TestGetOrCreateWatcher_DoesNotDeadlockOnReader is the regression test
// for the original bug. It mimics the production deadlock: a Rego query
// (the "reader") holds the OPA inmem store's RLock while a goroutine
// spawned by watchBucketBuiltin enters GetOrCreateWatcher and tries to
// Commit (which needs the store's WLock).
//
// Before the fix, GetOrCreateWatcher held gwm.mu (write) for the entire
// body, so the reader's next call to HasWatcher (gwm.mu.RLock) would
// block behind the spawned writer, the writer would block behind the
// reader's RLock, and the whole pod froze. After the fix, the writer
// holds only createMu while waiting on the store WLock; the reader's
// HasWatcher proceeds, the reader finishes its query and releases the
// store RLock, and the writer's Commit completes.
//
// This test asserts that GetOrCreateWatcher returns within a short
// deadline once the reader releases its RLock, with the reader making
// concurrent HasWatcher calls throughout the wait to verify they are
// not blocked by the in-flight create.
func TestGetOrCreateWatcher_DoesNotDeadlockOnReader(t *testing.T) {
	store := inmem.New()
	m := newTestManager(t, 8)
	// Override newWatcher to actually exercise the OPA store WLock — this
	// is what the real Start does via cleanOPAStore/Commit.
	m.newWatcher = func(bucketName string, s storage.Store) (*BucketWatcher, error) {
		ctx := context.Background()
		txn, err := s.NewTransaction(ctx, storage.WriteParams)
		if err != nil {
			return nil, err
		}
		if err := s.Commit(ctx, txn); err != nil {
			return nil, err
		}
		return &BucketWatcher{bucketName: bucketName}, nil
	}

	// The reader: open a long-lived read transaction.
	readerCtx := context.Background()
	readerTxn, err := store.NewTransaction(readerCtx)
	require.NoError(t, err)

	// The writer: try to create a watcher. With the fix, this blocks on
	// the store WLock until the reader closes its txn — but does NOT
	// deadlock with concurrent HasWatcher calls.
	created := make(chan error, 1)
	go func() {
		_, err := m.GetOrCreateWatcher("racy-bucket", store)
		created <- err
	}()

	// Concurrent HasWatcher calls must NOT block while the create is
	// waiting. Run a tight loop for a short window and assert each
	// returns quickly.
	hasWatcherStop := make(chan struct{})
	hasWatcherDone := make(chan struct{})
	go func() {
		defer close(hasWatcherDone)
		for {
			select {
			case <-hasWatcherStop:
				return
			default:
				start := time.Now()
				_ = m.HasWatcher("some-other-bucket")
				if d := time.Since(start); d > 100*time.Millisecond {
					t.Errorf("HasWatcher took %s while a create was in flight; that means gwm.mu is held across watcher.Start", d)
					return
				}
			}
		}
	}()

	// Sanity-check that the writer is in fact blocked.
	select {
	case <-created:
		t.Fatalf("GetOrCreateWatcher returned before reader released RLock — test setup is wrong")
	case <-time.After(200 * time.Millisecond):
		// good, writer is blocked on the store WLock
	}

	// Release the reader. The writer should complete promptly.
	store.Abort(readerCtx, readerTxn)

	select {
	case err := <-created:
		require.NoError(t, err, "create completed but with an error")
	case <-time.After(5 * time.Second):
		close(hasWatcherStop)
		<-hasWatcherDone
		t.Fatal("GetOrCreateWatcher did not complete within 5s after reader released RLock — deadlock")
	}

	close(hasWatcherStop)
	<-hasWatcherDone
}

// TestGetOrCreateWatcher_DoesNotDeadlockOnEviction is the regression
// test for the eviction-deadlock bug zeevmoney flagged in review:
// before the fix the LRU was created with NewWithEvict, so an Add or
// ContainsOrAdd at capacity would synchronously call onEviction →
// BucketWatcher.Stop → cleanOPAStore → Commit while gwm.mu.Lock was
// still held — the same circular wait as the original bug, just
// shifted from Start to eviction.
//
// We fill the cache to capacity, then call GetOrCreateWatcher for a
// new bucket (which forces an eviction) while a tight HasWatcher loop
// runs concurrently. If gwm.mu were held across the evicted watcher's
// Stop, HasWatcher (which takes gwm.mu.RLock) would block; the test
// asserts every HasWatcher call returns within 100ms.
//
// To make eviction visible and slow enough to race with HasWatcher,
// newWatcher itself sleeps briefly (so the create holds createMu for
// long enough that we can observe the lock interaction), and the
// pre-populated cache entries are inert *BucketWatcher{} values whose
// Stop short-circuits via !started — Stop is a no-op but the eviction
// path still runs through gwm.watchers.RemoveOldest under gwm.mu and
// then through evictedWatcher.Stop outside it.
func TestGetOrCreateWatcher_DoesNotDeadlockOnEviction(t *testing.T) {
	m := newTestManager(t, 1) // capacity=1 so any new bucket evicts the existing one
	m.watchers.Add("preloaded", &BucketWatcher{bucketName: "preloaded"})

	// Slow newWatcher gives the test a window to observe lock state.
	m.newWatcher = func(bucketName string, _ storage.Store) (*BucketWatcher, error) {
		time.Sleep(50 * time.Millisecond)
		return &BucketWatcher{bucketName: bucketName}, nil
	}

	created := make(chan error, 1)
	go func() {
		_, err := m.GetOrCreateWatcher("new-bucket", inmem.New())
		created <- err
	}()

	// Concurrent HasWatcher must remain responsive throughout — proves
	// gwm.mu is not held during the eviction's Stop call.
	hasStop := make(chan struct{})
	hasDone := make(chan struct{})
	go func() {
		defer close(hasDone)
		for {
			select {
			case <-hasStop:
				return
			default:
				start := time.Now()
				_ = m.HasWatcher("anything")
				if d := time.Since(start); d > 100*time.Millisecond {
					t.Errorf("HasWatcher took %s during eviction-in-flight; eviction is holding gwm.mu", d)
					return
				}
			}
		}
	}()

	select {
	case err := <-created:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		close(hasStop)
		<-hasDone
		t.Fatal("eviction-during-create did not complete within 5s — deadlock")
	}

	close(hasStop)
	<-hasDone

	// Cache should still contain exactly one watcher (the new one),
	// confirming the eviction actually happened.
	assert.Equal(t, 1, m.watchers.Len(), "cache should contain exactly the newly-created watcher")
	assert.True(t, m.HasWatcher("new-bucket"), "new bucket should be cached")
	assert.False(t, m.HasWatcher("preloaded"), "preloaded watcher should have been evicted")
}

// TestGetOrCreateWatcher_SerializesSameBucket asserts that N concurrent
// callers for the same bucket cause newWatcher to be invoked exactly
// once — proving createMu serializes creates and the double-check
// after acquiring it returns the canonical watcher to the loser(s).
func TestGetOrCreateWatcher_SerializesSameBucket(t *testing.T) {
	m := newTestManager(t, 8)

	// Slow create so there is a real window for racers to pile up on
	// createMu after the first caller acquired it.
	var creates int32
	m.newWatcher = func(bucketName string, _ storage.Store) (*BucketWatcher, error) {
		atomic.AddInt32(&creates, 1)
		time.Sleep(50 * time.Millisecond)
		return &BucketWatcher{bucketName: bucketName}, nil
	}

	const goroutines = 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	gotWatchers := make([]*BucketWatcher, goroutines)
	gotErrs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			gotWatchers[i], gotErrs[i] = m.GetOrCreateWatcher("racy", inmem.New())
		}()
	}
	wg.Wait()

	for i, err := range gotErrs {
		require.NoError(t, err, "caller %d", i)
	}
	first := gotWatchers[0]
	require.NotNil(t, first)
	for i := 1; i < goroutines; i++ {
		assert.Same(t, first, gotWatchers[i], "caller %d should have received the canonical watcher", i)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&creates), "newWatcher must be invoked exactly once across %d concurrent callers", goroutines)
}

// TestStop_DrainsInflightCreates asserts that Stop blocks until any
// in-flight GetOrCreateWatcher completes, and that subsequent creates
// are refused. This is the contract that prevents a watcher from being
// inserted into the cache after Stop has torn the manager down.
func TestStop_DrainsInflightCreates(t *testing.T) {
	m := newTestManager(t, 8)

	// Block the create until the test releases the gate. Stop should not
	// return until after the gate is released and the create completes.
	gate := make(chan struct{})
	createReturned := make(chan struct{})
	m.newWatcher = func(bucketName string, _ storage.Store) (*BucketWatcher, error) {
		<-gate
		return &BucketWatcher{bucketName: bucketName}, nil
	}

	go func() {
		defer close(createReturned)
		_, _ = m.GetOrCreateWatcher("inflight", inmem.New())
	}()

	// Give the create a moment to acquire createMu.
	time.Sleep(50 * time.Millisecond)

	stopReturned := make(chan struct{})
	go func() {
		defer close(stopReturned)
		_ = m.Stop()
	}()

	// Stop must not return while the create is still in flight.
	select {
	case <-stopReturned:
		t.Fatal("Stop returned while a create was still in flight")
	case <-time.After(150 * time.Millisecond):
		// good
	}

	// Release the create. Stop should now be able to proceed.
	close(gate)

	select {
	case <-createReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight create did not return after gate released")
	}
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after in-flight create completed")
	}

	// Subsequent creates must be refused.
	_, err := m.GetOrCreateWatcher("post-stop", inmem.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopping")
}
