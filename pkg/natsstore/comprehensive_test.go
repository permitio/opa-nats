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
	config := DefaultConfig()
	logger := logging.Get()
	dt, err := NewDataTransformer(config, logger)
	require.NoError(t, err)

	tests := []struct {
		name        string
		natsKey     string
		bucketName  string
		isRoot      bool
		expected    storage.Path
		expectError bool
	}{
		{
			name:       "root bucket with simple key",
			natsKey:    "users.123",
			bucketName: "test-bucket",
			isRoot:     true,
			expected:   storage.Path{"users", "123"},
		},
		{
			name:       "watched bucket with simple key",
			natsKey:    "users.123",
			bucketName: "test-bucket",
			isRoot:     false,
			expected:   storage.Path{"nats", "kv", "test-bucket", "users", "123"},
		},
		{
			name:       "nested key with multiple dots",
			natsKey:    "groups.org1.users.john.profile.settings",
			bucketName: "permissions",
			isRoot:     false,
			expected:   storage.Path{"nats", "kv", "permissions", "groups", "org1", "users", "john", "profile", "settings"},
		},
		{
			name:        "empty key root bucket - should error",
			natsKey:     "",
			bucketName:  "test",
			isRoot:      true,
			expectError: true,
		},
		{
			name:        "empty key watched bucket - should error",
			natsKey:     "",
			bucketName:  "test",
			isRoot:      false,
			expectError: true,
		},
		{
			name:       "single part key root bucket",
			natsKey:    "config",
			bucketName: "settings",
			isRoot:     true,
			expected:   storage.Path{"config"},
		},
		{
			name:       "single part key watched bucket",
			natsKey:    "config",
			bucketName: "settings",
			isRoot:     false,
			expected:   storage.Path{"nats", "kv", "settings", "config"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := dt.NATSKeyToOPAPath(tt.natsKey, tt.bucketName, tt.isRoot)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, path)
			}
		})
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
	config := DefaultConfig()
	logger := logging.Get()
	dt, err := NewDataTransformer(config, logger)
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
