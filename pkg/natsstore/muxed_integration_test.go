package natsstore

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dialJetStreamOrSkip connects to a local NATS server with JetStream, or skips
// the test when none is reachable (so the unit suite stays runnable offline).
func dialJetStreamOrSkip(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect("nats://localhost:4222", nats.Timeout(time.Second))
	if err != nil {
		t.Skipf("no NATS server at localhost:4222: %v", err)
	}
	js, err := nc.JetStream()
	require.NoError(t, err)
	return nc, js
}

// TestMuxedTenantIsolation_Integration exercises the real NATS-touching paths:
// a single muxed bucket "DATA" with keys "<tenant>.<...>", verifying that
// per-tenant access reads ONLY that tenant's slice and lands at the same OPA
// path as the old bucket-per-tenant layout (data.nats.kv.<tenant>.<...>).
func TestMuxedTenantIsolation_Integration(t *testing.T) {
	nc, js := dialJetStreamOrSkip(t)
	defer nc.Close()

	_ = js.DeleteKeyValue("DATA")
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "DATA", History: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteKeyValue("DATA") })

	_, err = kv.PutString("t1.members", `["alice","bob"]`)
	require.NoError(t, err)
	_, err = kv.PutString("t1.profile.name", `"acme"`)
	require.NoError(t, err)
	_, err = kv.PutString("t2.secret", `"must-not-leak"`)
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.Bucket = "DATA"
	client, err := NewNATSClient(cfg, logging.Get())
	require.NoError(t, err)

	t.Run("tenantKeys returns only the tenant's slice", func(t *testing.T) {
		k1, err := client.tenantKeys("t1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"t1.members", "t1.profile.name"}, k1)

		k2, err := client.tenantKeys("t2")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"t2.secret"}, k2)

		// an unknown tenant gets nothing (not the whole bucket)
		k3, err := client.tenantKeys("t9")
		require.NoError(t, err)
		assert.Empty(t, k3)
	})

	t.Run("bulk load places tenant data at data.nats.kv.<tenant> and excludes others", func(t *testing.T) {
		dt, err := NewDataTransformer(cfg, logging.Get())
		require.NoError(t, err)
		store := NewMockStore()

		require.NoError(t, dt.LoadBucketDataBulk(context.Background(), "t1", client, store, false))

		members, err := store.Read(context.Background(), nil, storage.Path{"nats", "kv", "t1", "members"})
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"alice", "bob"}, members)

		name, err := store.Read(context.Background(), nil, storage.Path{"nats", "kv", "t1", "profile", "name"})
		require.NoError(t, err)
		assert.Equal(t, "acme", name)

		// tenant 2's secret must never have been loaded by a t1 load
		_, err = store.Read(context.Background(), nil, storage.Path{"nats", "kv", "t2", "secret"})
		assert.True(t, storage.IsNotFound(err), "t2 data must not leak into a t1 load, got err=%v", err)
	})

	t.Run("root tenant strips the tenant token to the data root", func(t *testing.T) {
		dt, err := NewDataTransformer(cfg, logging.Get())
		require.NoError(t, err)
		store := NewMockStore()

		require.NoError(t, dt.LoadBucketDataBulk(context.Background(), "t1", client, store, true))

		// root mode: data.<rest> (no nats/kv/<tenant> prefix)
		members, err := store.Read(context.Background(), nil, storage.Path{"members"})
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"alice", "bob"}, members)
	})
}
