package natsstore

import (
	"context"
	"testing"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Test DataTransformer functions that don't require NATS connection

func TestDataTransformer_NATSKeyToOPAPath_Comprehensive(t *testing.T) {
	logger := logging.Get()
	dt, err := NewDataTransformer(logger)
	require.NoError(t, err)

	// Single muxed bucket: the NATS key is the FULL muxed key "<tenant>.<rest...>".
	// The tenant token is the first segment and becomes the bucket segment of the
	// OPA path, so placement is identical to the old bucket-per-tenant layout
	// (data.nats.kv.<tenant>.<rest>) without passing the bucket name out-of-band.
	// In root mode the tenant token is stripped and the remainder mounts at data root.
	tests := []struct {
		name        string
		fullKey     string
		isRoot      bool
		expected    storage.Path
		expectError bool
	}{
		{
			name:     "non-root: tenant becomes the kv segment",
			fullKey:  "test-bucket.users.123",
			isRoot:   false,
			expected: storage.Path{"nats", "kv", "test-bucket", "users", "123"},
		},
		{
			name:     "root: tenant token stripped, remainder at data root",
			fullKey:  "test-bucket.users.123",
			isRoot:   true,
			expected: storage.Path{"users", "123"},
		},
		{
			name:     "non-root: deeply nested key",
			fullKey:  "permissions.groups.org1.users.john.profile.settings",
			isRoot:   false,
			expected: storage.Path{"nats", "kv", "permissions", "groups", "org1", "users", "john", "profile", "settings"},
		},
		{
			name:        "empty key errors (non-root)",
			fullKey:     "",
			isRoot:      false,
			expectError: true,
		},
		{
			name:        "empty key errors (root)",
			fullKey:     "",
			isRoot:      true,
			expectError: true,
		},
		{
			name:     "non-root: single sub-key under tenant",
			fullKey:  "settings.config",
			isRoot:   false,
			expected: storage.Path{"nats", "kv", "settings", "config"},
		},
		{
			name:     "root: single sub-key under tenant",
			fullKey:  "settings.config",
			isRoot:   true,
			expected: storage.Path{"config"},
		},
		{
			name:        "root: tenant token only, nothing to mount - errors",
			fullKey:     "settings",
			isRoot:      true,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := dt.NATSKeyToOPAPath(tt.fullKey, tt.isRoot)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, path)
			}
		})
	}
}

// buildTenantJSON builds a tenant-relative JSON document from muxed keys,
// stripping the "<tenant>." prefix and excluding other tenants' keys.
func TestBuildTenantJSON(t *testing.T) {
	store := map[string][]byte{
		"t1.members":      []byte(`["alice","bob"]`),
		"t1.profile.name": []byte(`"acme"`),
		"t2.secret":       []byte(`"must-not-leak"`),
	}
	// caller passes only the tenant's filtered keys, but the builder must also
	// defend against a stray foreign key sneaking in.
	keys := []string{"t1.members", "t1.profile.name", "t2.secret"}
	get := func(k string) []byte { return store[k] }

	raw := buildTenantJSON("t1", keys, get)
	res := gjson.ParseBytes(raw)

	assert.Equal(t, []interface{}{"alice", "bob"}, res.Get("members").Value())
	assert.Equal(t, "acme", res.Get("profile.name").String())
	// tenant prefix is stripped: tenant-qualified key must NOT appear
	assert.False(t, res.Get("t1.members").Exists())
	// other tenants are excluded entirely
	assert.False(t, res.Get("secret").Exists())
	assert.False(t, res.Get("t2.secret").Exists())
}

func TestBuildTenantJSON_SkipsFailedGetAndQuotesNonJSON(t *testing.T) {
	keys := []string{"t1.ok", "t1.failed", "t1.plain"}
	get := func(k string) []byte {
		switch k {
		case "t1.ok":
			return []byte(`{"a":1}`)
		case "t1.failed":
			return nil // get failed for this key
		case "t1.plain":
			return []byte("not-json")
		}
		return nil
	}

	res := gjson.ParseBytes(buildTenantJSON("t1", keys, get))

	// a failed get omits the key entirely — it must NOT become an explicit null
	// (a key flipping to null can change an authz decision).
	assert.False(t, res.Get("failed").Exists())
	// valid JSON is preserved as-is
	assert.Equal(t, float64(1), res.Get("ok.a").Value())
	// a non-JSON value is stored as a JSON string (mirrors loadSingleKey) instead
	// of corrupting the whole document
	assert.Equal(t, gjson.String, res.Get("plain").Type)
	assert.Equal(t, "not-json", res.Get("plain").String())
}

func TestValidateTenant(t *testing.T) {
	// real tenants are env UUID hex; these must pass
	for _, v := range []string{"550e8400e29b41d4a716446655440000", "abc", "a-b_c", "ABC123"} {
		assert.NoError(t, validateTenant(v), "expected %q to be valid", v)
	}
	// anything that isn't a single safe NATS subject token must be rejected,
	// so the subject-token watch and the string-prefix read can't diverge
	for _, v := range []string{"", "a.b", ".", "*", ">", "a*", "a>b", "a b", "a\tb", "a\nb"} {
		assert.Error(t, validateTenant(v), "expected %q to be rejected", v)
	}
}

// Test NATSClient functions that don't require connection

func TestNATSClient_keyToPath_Comprehensive(t *testing.T) {
	client := &NATSClient{}

	tests := []struct {
		name     string
		key      string
		expected []string
	}{
		{
			name:     "simple key with two parts",
			key:      "users.123",
			expected: []string{"users", "123"},
		},
		{
			name:     "nested key with multiple parts",
			key:      "groups.org1.users.john.profile",
			expected: []string{"groups", "org1", "users", "john", "profile"},
		},
		{
			name:     "empty key",
			key:      "",
			expected: []string{},
		},
		{
			name:     "single part key",
			key:      "config",
			expected: []string{"config"},
		},
		{
			name:     "key with many parts",
			key:      "a.b.c.d.e.f.g.h.i.j",
			expected: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
		},
		{
			name:     "key with special characters",
			key:      "user-123.profile_data.settings",
			expected: []string{"user-123", "profile_data", "settings"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.keyToPath(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNATSClient_setConnected_Comprehensive(t *testing.T) {
	client := &NATSClient{}

	// Test initial state
	assert.False(t, client.connected)

	// Test setting to true
	client.setConnected(true)
	assert.True(t, client.connected)

	// Test setting to false
	client.setConnected(false)
	assert.False(t, client.connected)

	// Test multiple toggles
	for i := 0; i < 5; i++ {
		client.setConnected(true)
		assert.True(t, client.connected)
		client.setConnected(false)
		assert.False(t, client.connected)
	}
}

// Test Plugin factory functions that don't require complex setup

func TestGetDataCacheKey_Comprehensive(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		expected   string
	}{
		{
			name:       "simple bucket name",
			bucketName: "users",
			expected:   "nats.kv.users",
		},
		{
			name:       "complex bucket name",
			bucketName: "permissions-bucket",
			expected:   "nats.kv.permissions-bucket",
		},
		{
			name:       "empty bucket name",
			bucketName: "",
			expected:   "nats.kv.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDataCacheKey(tt.bucketName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test MockStore functions to increase coverage

func TestMockStore_Comprehensive(t *testing.T) {
	store := NewMockStore()
	assert.NotNil(t, store)

	ctx := context.Background()

	// Test NewTransaction
	txn, err := store.NewTransaction(ctx, storage.TransactionParams{
		Context: storage.NewContext(),
		Write:   true,
	})
	require.NoError(t, err)
	assert.NotNil(t, txn)

	// Test transaction ID
	id := txn.ID()
	assert.Greater(t, id, uint64(0))

	// Test Read with non-existent path
	_, err = store.Read(ctx, txn, storage.Path{"nonexistent"})
	assert.Error(t, err)

	// Test Write operation
	testPath := storage.Path{"test", "data"}
	testValue := "test-value"
	err = store.Write(ctx, txn, storage.AddOp, testPath, testValue)
	require.NoError(t, err)

	// Test Read after Write
	value, err := store.Read(ctx, txn, testPath)
	require.NoError(t, err)
	assert.Equal(t, testValue, value)

	// Test Write with ReplaceOp
	newValue := "new-test-value"
	err = store.Write(ctx, txn, storage.ReplaceOp, testPath, newValue)
	require.NoError(t, err)

	value, err = store.Read(ctx, txn, testPath)
	require.NoError(t, err)
	assert.Equal(t, newValue, value)

	// Test Write with RemoveOp
	err = store.Write(ctx, txn, storage.RemoveOp, testPath, nil)
	require.NoError(t, err)

	_, err = store.Read(ctx, txn, testPath)
	assert.Error(t, err) // Should not exist after removal

	// Test Commit
	err = store.Commit(ctx, txn)
	assert.NoError(t, err)

	// Test Abort (after commit should be no-op)
	store.Abort(ctx, txn)

	// Test policy operations
	policies, err := store.ListPolicies(ctx, txn)
	assert.NoError(t, err)
	assert.Empty(t, policies)

	policy, err := store.GetPolicy(ctx, txn, "test-policy")
	assert.Error(t, err)
	assert.Nil(t, policy)

	err = store.UpsertPolicy(ctx, txn, "test-policy", []byte("package test"))
	assert.NoError(t, err)

	err = store.DeletePolicy(ctx, txn, "test-policy")
	assert.NoError(t, err)

	// Test register/unregister operations (simplified to just test they don't panic)
	_, err = store.Register(ctx, txn, storage.TriggerConfig{})
	assert.NoError(t, err)
	// store.Unregister doesn't exist in our mock or we need a proper handle

	// Test Close
	store.Close(ctx, txn)

	// Test Truncate with empty iterator
	// This needs proper parameters, but let's skip for now since it's complex
}

func TestMockStore_pathToKey(t *testing.T) {
	tests := []struct {
		name     string
		path     storage.Path
		expected string
	}{
		{
			name:     "simple path",
			path:     storage.Path{"users", "123"},
			expected: "users.123",
		},
		{
			name:     "nested path",
			path:     storage.Path{"groups", "org1", "users", "john"},
			expected: "groups.org1.users.john",
		},
		{
			name:     "single element path",
			path:     storage.Path{"config"},
			expected: "config",
		},
		{
			name:     "empty path",
			path:     storage.Path{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathToKey(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMockStore_DataOperations(t *testing.T) {
	store := NewMockStore()

	// Test GetData with empty store
	data := store.GetData()
	assert.Empty(t, data)

	// Test SetData
	testData := map[string]interface{}{
		"users": map[string]interface{}{
			"123": map[string]interface{}{
				"name": "John Doe",
				"age":  30,
			},
		},
	}
	store.SetData(testData)

	// Test GetData after SetData
	retrievedData := store.GetData()
	assert.Equal(t, testData, retrievedData)

	// Test Clear
	store.Clear()
	clearedData := store.GetData()
	assert.Empty(t, clearedData)
}

// Test some simple error conditions and edge cases

func TestDataTransformer_ensureParentPaths_EdgeCases(t *testing.T) {
	logger := logging.Get()
	dt, err := NewDataTransformer(logger)
	require.NoError(t, err)

	store := NewMockStore()
	ctx := context.Background()
	txn, err := store.NewTransaction(ctx, storage.TransactionParams{
		Context: storage.NewContext(),
		Write:   true,
	})
	require.NoError(t, err)

	// Test with empty path - should not error
	err = dt.ensureParentPaths(ctx, storage.Path{}, store, txn)
	assert.NoError(t, err)

	// Test with single element path - should not error
	err = dt.ensureParentPaths(ctx, storage.Path{"single"}, store, txn)
	assert.NoError(t, err)

	// Test with two element path - should create parent
	err = dt.ensureParentPaths(ctx, storage.Path{"parent", "child"}, store, txn)
	assert.NoError(t, err)
}

func TestConfig_EdgeCases(t *testing.T) {
	// Test UnmarshalJSON with edge cases that might not be covered
	tests := []struct {
		name        string
		jsonStr     string
		expectError bool
	}{
		{
			name:        "malformed JSON",
			jsonStr:     "not-json",
			expectError: true,
		},
		{
			name:        "empty string",
			jsonStr:     `""`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := d.UnmarshalJSON([]byte(tt.jsonStr))
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Add some tests for functions that involve complex operations but can be partially tested

func TestPluginFactory_gjsonResultToASTTerm_EdgeCases(t *testing.T) {
	factory := NewPluginFactory()

	// Test with various gjson result types that might not be covered
	tests := []struct {
		name      string
		jsonData  string
		gjsonPath string
	}{
		{
			name:      "null value",
			jsonData:  `{"test": null}`,
			gjsonPath: "test",
		},
		{
			name:      "boolean true",
			jsonData:  `{"test": true}`,
			gjsonPath: "test",
		},
		{
			name:      "boolean false",
			jsonData:  `{"test": false}`,
			gjsonPath: "test",
		},
		{
			name:      "number",
			jsonData:  `{"test": 42}`,
			gjsonPath: "test",
		},
		{
			name:      "string",
			jsonData:  `{"test": "hello"}`,
			gjsonPath: "test",
		},
		{
			name:      "array",
			jsonData:  `{"test": [1, 2, 3]}`,
			gjsonPath: "test",
		},
		{
			name:      "object",
			jsonData:  `{"test": {"nested": "value"}}`,
			gjsonPath: "test",
		},
		{
			name:      "nonexistent path",
			jsonData:  `{"test": "value"}`,
			gjsonPath: "nonexistent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gjson.Get(tt.jsonData, tt.gjsonPath)
			term := factory.gjsonResultToASTTerm(result)
			assert.NotNil(t, term)
		})
	}
}
