package natsstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logRecord is a single captured log emission.
type logRecord struct {
	level   logging.Level
	message string
}

// recordingLogger implements logging.Logger and keeps every formatted message
// together with the level it was emitted at, so tests can assert not just
// "something was logged" but "it was visible at the default (Info) level".
type recordingLogger struct {
	mu      sync.Mutex
	records []logRecord
}

func (r *recordingLogger) record(level logging.Level, format string, a ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, logRecord{level: level, message: fmt.Sprintf(format, a...)})
}

func (r *recordingLogger) Debug(format string, a ...any)              { r.record(logging.Debug, format, a...) }
func (r *recordingLogger) Info(format string, a ...any)               { r.record(logging.Info, format, a...) }
func (r *recordingLogger) Warn(format string, a ...any)               { r.record(logging.Warn, format, a...) }
func (r *recordingLogger) Error(format string, a ...any)              { r.record(logging.Error, format, a...) }
func (r *recordingLogger) WithFields(_ map[string]any) logging.Logger { return r }
func (r *recordingLogger) GetLevel() logging.Level                    { return logging.Info }
func (r *recordingLogger) SetLevel(_ logging.Level)                   {}

// atLevel returns the messages captured at exactly the given level.
func (r *recordingLogger) atLevel(level logging.Level) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, rec := range r.records {
		if rec.level == level {
			out = append(out, rec.message)
		}
	}
	return out
}

// all returns every captured message regardless of level.
func (r *recordingLogger) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.message)
	}
	return out
}

// fakeKVEntry is a nats.KeyValueEntry that carries exactly the fields
// handleKVUpdate reads, so updates can be driven without a NATS server.
type fakeKVEntry struct {
	key      string
	value    []byte
	revision uint64
	op       nats.KeyValueOp
}

func (f fakeKVEntry) Bucket() string             { return "permit" }
func (f fakeKVEntry) Key() string                { return f.key }
func (f fakeKVEntry) Value() []byte              { return f.value }
func (f fakeKVEntry) Revision() uint64           { return f.revision }
func (f fakeKVEntry) Created() time.Time         { return time.Time{} }
func (f fakeKVEntry) Delta() uint64              { return 0 }
func (f fakeKVEntry) Operation() nats.KeyValueOp { return f.op }

// newLoggingTestWatcher builds a BucketWatcher wired to a real in-memory OPA
// store and a recording logger — enough for handleKVUpdate, which needs no
// NATS connection of its own (the entry is handed to it directly).
func newLoggingTestWatcher(t *testing.T, tenant string, isRoot bool) (*BucketWatcher, *recordingLogger, storage.Store) {
	t.Helper()
	logger := &recordingLogger{}
	transformer, err := NewDataTransformer(logger)
	require.NoError(t, err)
	store := inmem.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &BucketWatcher{
		bucketName:        tenant,
		dataTransformer:   transformer,
		opaStore:          store,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
		isRoot:            isRoot,
		stopReq:           make(chan struct{}),
		watcherLoopExited: make(chan struct{}),
	}, logger, store
}

// TestHandleKVUpdate_LogsAppliedUpdatesAtInfo covers the PER-15709 acceptance
// criteria: every applied operation is visible at the default log level and
// carries tenant, key, OPA path, operation and revision.
func TestHandleKVUpdate_LogsAppliedUpdatesAtInfo(t *testing.T) {
	tests := []struct {
		name     string
		entry    fakeKVEntry
		wantOp   string
		wantPath string
	}{
		{
			name:     "put",
			entry:    fakeKVEntry{key: "t1.users.123", value: []byte(`{"role":"admin"}`), revision: 7, op: nats.KeyValuePut},
			wantOp:   "put",
			wantPath: "/nats/kv/t1/users/123",
		},
		{
			name:     "delete",
			entry:    fakeKVEntry{key: "t1.users.123", revision: 8, op: nats.KeyValueDelete},
			wantOp:   "delete",
			wantPath: "/nats/kv/t1/users/123",
		},
		{
			name:     "purge",
			entry:    fakeKVEntry{key: "t1.users.123", revision: 9, op: nats.KeyValuePurge},
			wantOp:   "purge",
			wantPath: "/nats/kv/t1/users/123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw, logger, _ := newLoggingTestWatcher(t, "t1", false)

			gw.handleKVUpdate(tt.entry)

			infos := logger.atLevel(logging.Info)
			require.Len(t, infos, 1, "expected exactly one Info log per applied update, got: %v", infos)
			msg := infos[0]
			assert.Contains(t, msg, "tenant=t1")
			assert.Contains(t, msg, "key=t1.users.123")
			assert.Contains(t, msg, "path="+tt.wantPath)
			assert.Contains(t, msg, "revision="+fmt.Sprint(tt.entry.revision))
			assert.Contains(t, msg, tt.wantOp)

			assert.Empty(t, logger.atLevel(logging.Error), "applied update must not log an error")
		})
	}
}

// TestHandleKVUpdate_LogsRootTenantPath verifies the Info log reports the
// root-tenant OPA path (tenant token stripped, mounted at the data root)
// rather than the raw key split on dots.
func TestHandleKVUpdate_LogsRootTenantPath(t *testing.T) {
	gw, logger, _ := newLoggingTestWatcher(t, "root", true)

	gw.handleKVUpdate(fakeKVEntry{key: "root.policies.abac", value: []byte(`{"x":1}`), revision: 3, op: nats.KeyValuePut})

	infos := logger.atLevel(logging.Info)
	require.Len(t, infos, 1)
	assert.Contains(t, infos[0], "path=/policies/abac")
}

// TestHandleKVUpdate_NeverLogsPayload guards the "do not log policy data"
// requirement: no captured message, at any level, may contain the value.
func TestHandleKVUpdate_NeverLogsPayload(t *testing.T) {
	const secret = "s3cret-policy-payload"
	gw, logger, _ := newLoggingTestWatcher(t, "t1", false)

	gw.handleKVUpdate(fakeKVEntry{
		key:      "t1.users.123",
		value:    []byte(`{"token":"` + secret + `"}`),
		revision: 1,
		op:       nats.KeyValuePut,
	})

	for _, msg := range logger.all() {
		assert.NotContains(t, msg, secret, "policy payload must never be logged")
	}
}

// TestHandleKVUpdate_AppliesUpdateToStore keeps the logging change honest:
// the Info log is only emitted after the write actually reached OPA.
func TestHandleKVUpdate_AppliesUpdateToStore(t *testing.T) {
	gw, logger, store := newLoggingTestWatcher(t, "t1", false)

	gw.handleKVUpdate(fakeKVEntry{key: "t1.users.123", value: []byte(`{"role":"admin"}`), revision: 4, op: nats.KeyValuePut})
	require.Len(t, logger.atLevel(logging.Info), 1)

	value, err := storage.ReadOne(gw.ctx, store, storage.Path{"nats", "kv", "t1", "users", "123"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"role": "admin"}, value)
}

// TestHandleKVUpdate_UnmappableKeyLogsErrorNotInfo verifies that a key which
// cannot be mapped to an OPA path is reported as an error and does NOT produce
// a success log claiming the update was applied.
func TestHandleKVUpdate_UnmappableKeyLogsErrorNotInfo(t *testing.T) {
	gw, logger, _ := newLoggingTestWatcher(t, "root", true)

	// A root key with no sub-key beyond the tenant token has no OPA path.
	gw.handleKVUpdate(fakeKVEntry{key: "root", value: []byte(`{}`), revision: 2, op: nats.KeyValuePut})

	assert.Empty(t, logger.atLevel(logging.Info), "a failed update must not log as applied")
	errs := logger.atLevel(logging.Error)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "root")
}

// TestHandleKVUpdate_UnknownOperationWarns verifies that an operation this
// plugin does not handle is surfaced rather than dropped silently.
func TestHandleKVUpdate_UnknownOperationWarns(t *testing.T) {
	gw, logger, _ := newLoggingTestWatcher(t, "t1", false)

	gw.handleKVUpdate(fakeKVEntry{key: "t1.users.123", revision: 5, op: nats.KeyValueOp(200)})

	assert.Empty(t, logger.atLevel(logging.Info))
	warns := logger.atLevel(logging.Warn)
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], "unsupported")
}

// TestKVOpName pins the short operation tokens used in the logs, which
// operators (and log-based alerts) grep for.
func TestKVOpName(t *testing.T) {
	assert.Equal(t, "put", kvOpName(nats.KeyValuePut))
	assert.Equal(t, "delete", kvOpName(nats.KeyValueDelete))
	assert.Equal(t, "purge", kvOpName(nats.KeyValuePurge))
	assert.True(t, strings.HasPrefix(kvOpName(nats.KeyValueOp(200)), "unknown("))
}
