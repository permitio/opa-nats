package natsstore

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"sync"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
)

// Helper function to create a composite store for testing path routing
func createTestCompositeStore(handledPathsRegex []string) *SimpleCompositeStore {
	mockStore := NewMockStore()
	logger := logging.Get()
	// Pass empty group regex pattern and single group for tests that don't need them
	return NewSimpleCompositeStore(mockStore, nil, logger, handledPathsRegex, "", "")
}

func TestSimpleCompositeStore_PathRoutingLogic(t *testing.T) {
	t.Run("with regex patterns", func(t *testing.T) {
		// Test with regex patterns - should only match regex, no defaults
		regexPatterns := []string{
			"^data/schemas($|/.*)",
			"^data/custom($|/.*)",
		}
		store := createTestCompositeStore(regexPatterns)

		tests := []struct {
			name     string
			path     storage.Path
			expected bool
		}{
			{
				name:     "empty path",
				path:     storage.Path{},
				expected: false,
			},
			{
				name:     "data.schemas path (matches regex)",
				path:     storage.Path{"data", "schemas"},
				expected: true,
			},
			{
				name:     "data.schemas.user path (matches regex)",
				path:     storage.Path{"data", "schemas", "user"},
				expected: true,
			},
			{
				name:     "data.custom.test path (matches regex)",
				path:     storage.Path{"data", "custom", "test"},
				expected: true,
			},
			{
				name:     "data.resource_types path (no regex match)",
				path:     storage.Path{"data", "resource_types"},
				expected: false,
			},
			{
				name:     "policy path",
				path:     storage.Path{"policy"},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := store.shouldUseNATS(tt.path)
				if result != tt.expected {
					t.Errorf("Expected %v for path %v, got %v", tt.expected, tt.path, result)
				}
			})
		}
	})

	t.Run("with empty regex (no defaults)", func(t *testing.T) {
		// Test with no regex patterns - should not match anything per README
		store := createTestCompositeStore([]string{})

		tests := []struct {
			name     string
			path     storage.Path
			expected bool
		}{
			{
				name:     "data.schemas path (no match without regex)",
				path:     storage.Path{"data", "schemas"},
				expected: false,
			},
			{
				name:     "data.resource_types path (no match without regex)",
				path:     storage.Path{"data", "resource_types"},
				expected: false,
			},
			{
				name:     "any path should not match",
				path:     storage.Path{"data", "anything"},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := store.shouldUseNATS(tt.path)
				if result != tt.expected {
					t.Errorf("Expected %v for path %v, got %v", tt.expected, tt.path, result)
				}
			})
		}
	})
}

func TestSimpleCompositeStore_MockStoreOperations(t *testing.T) {
	// Test the mock store directly
	mockStore := NewMockStore()
	ctx := context.Background()

	// Test transaction creation
	txn, err := mockStore.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	if txn == nil {
		t.Error("Transaction should not be nil")
	}

	if txn.ID() == 0 {
		t.Error("Transaction ID should not be 0")
	}

	// Test write operation
	path := storage.Path{"policy", "test"}
	value := map[string]interface{}{"rules": []string{"allow", "deny"}}

	err = mockStore.Write(ctx, txn, storage.AddOp, path, value)
	if err != nil {
		t.Errorf("Write should not return error: %v", err)
	}

	// Test read operation
	result, err := mockStore.Read(ctx, txn, path)
	if err != nil {
		t.Errorf("Read should not return error: %v", err)
	}

	if !reflect.DeepEqual(result, value) {
		t.Errorf("Expected result %v, got %v", value, result)
	}

	// Test read non-existent path
	nonExistentPath := storage.Path{"non", "existent"}
	_, err = mockStore.Read(ctx, txn, nonExistentPath)
	if err == nil {
		t.Error("Read should return error for non-existent path")
	}

	// Test remove operation
	err = mockStore.Write(ctx, txn, storage.RemoveOp, path, nil)
	if err != nil {
		t.Errorf("Remove should not return error: %v", err)
	}

	// Verify removal
	_, err = mockStore.Read(ctx, txn, path)
	if err == nil {
		t.Error("Read should return error after removal")
	}

	// Test commit
	err = mockStore.Commit(ctx, txn)
	if err != nil {
		t.Errorf("Commit should not return error: %v", err)
	}
}

func TestSimpleCompositeStore_MockStorePolicyOperations(t *testing.T) {
	mockStore := NewMockStore()
	ctx := context.Background()

	txn, err := mockStore.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	// Test policy operations
	policies, err := mockStore.ListPolicies(ctx, txn)
	if err != nil {
		t.Errorf("ListPolicies should not return error: %v", err)
	}

	if len(policies) != 0 {
		t.Error("Expected empty policies list")
	}

	// Test get non-existent policy
	_, err = mockStore.GetPolicy(ctx, txn, "test-policy")
	if err == nil {
		t.Error("GetPolicy should return error for non-existent policy")
	}

	// Test upsert policy
	err = mockStore.UpsertPolicy(ctx, txn, "test-policy", []byte("package test"))
	if err != nil {
		t.Errorf("UpsertPolicy should not return error: %v", err)
	}

	// Test delete policy
	err = mockStore.DeletePolicy(ctx, txn, "test-policy")
	if err != nil {
		t.Errorf("DeletePolicy should not return error: %v", err)
	}
}

func TestSimpleCompositeStore_MockStoreTriggerOperations(t *testing.T) {
	mockStore := NewMockStore()
	ctx := context.Background()

	txn, err := mockStore.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	// Test trigger operations
	config := storage.TriggerConfig{
		OnCommit: func(ctx context.Context, txn storage.Transaction, event storage.TriggerEvent) {
			// Test callback
		},
	}

	handle, err := mockStore.Register(ctx, txn, config)
	if err != nil {
		t.Errorf("Register should not return error: %v", err)
	}

	// Test unregister
	mockStore.Unregister(ctx, txn, handle)

	// Test abort
	mockStore.Abort(ctx, txn)
}

func TestSimpleCompositeStore_MockStoreHelperMethods(t *testing.T) {
	mockStore := NewMockStore()

	// Test initial state
	data := mockStore.GetData()
	if len(data) != 0 {
		t.Error("Expected empty data initially")
	}

	// Test set data
	testData := map[string]interface{}{
		"key1": "value1",
		"key2": map[string]interface{}{"nested": "value"},
	}

	mockStore.SetData(testData)

	// Test get data
	retrievedData := mockStore.GetData()
	if len(retrievedData) != 2 {
		t.Errorf("Expected 2 items, got %d", len(retrievedData))
	}

	if retrievedData["key1"] != "value1" {
		t.Errorf("Expected value1, got %v", retrievedData["key1"])
	}

	// Test clear
	mockStore.Clear()

	data = mockStore.GetData()
	if len(data) != 0 {
		t.Error("Expected empty data after clear")
	}
}

func TestSimpleCompositeStore_MockNATSClientOperations(t *testing.T) {
	logger := logging.Get()
	mockClient := NewMockNATSClient(logger)
	ctx := context.Background()

	// Test initial state
	if mockClient.IsStarted() {
		t.Error("Client should not be started initially")
	}

	// Test start
	err := mockClient.Start(ctx)
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}

	if !mockClient.IsStarted() {
		t.Error("Client should be started after Start()")
	}

	// Test put and get
	path := []string{"data", "schemas", "user"}
	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{"type": "string"},
		},
	}

	err = mockClient.Put(path, value)
	if err != nil {
		t.Errorf("Put should not return error: %v", err)
	}

	retrievedValue, err := mockClient.Get(path)
	if err != nil {
		t.Errorf("Get should not return error: %v", err)
	}

	if !reflect.DeepEqual(retrievedValue, value) {
		t.Errorf("Expected value %v, got %v", value, retrievedValue)
	}

	// Test get non-existent key
	_, err = mockClient.Get([]string{"non", "existent"})
	if err == nil {
		t.Error("Get should return error for non-existent key")
	}

	// Test delete
	err = mockClient.Delete(path)
	if err != nil {
		t.Errorf("Delete should not return error: %v", err)
	}

	_, err = mockClient.Get(path)
	if err == nil {
		t.Error("Get should return error after delete")
	}

	// Test stop
	err = mockClient.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}

	if mockClient.IsStarted() {
		t.Error("Client should not be started after Stop()")
	}
}

func TestSimpleCompositeStore_MockNATSClientUpdateCallback(t *testing.T) {
	logger := logging.Get()
	mockClient := NewMockNATSClient(logger)
	ctx := context.Background()

	err := mockClient.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Test update callback
	var mu sync.Mutex
	callbackCalled := false
	var callbackKey string
	var callbackValue interface{}

	mockClient.SetUpdateCallback(func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		callbackCalled = true
		callbackKey = key
		callbackValue = value
	})

	// Put data to trigger callback
	path := []string{"data", "test", "callback"}
	value := "callback test"

	err = mockClient.Put(path, value)
	if err != nil {
		t.Errorf("Put should not return error: %v", err)
	}

	// Wait for callback
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	called := callbackCalled
	key := callbackKey
	val := callbackValue
	mu.Unlock()

	if !called {
		t.Error("Update callback should have been called")
	}

	expectedKey := "data.test.callback"
	if key != expectedKey {
		t.Errorf("Expected callback key %s, got %s", expectedKey, key)
	}

	if val != value {
		t.Errorf("Expected callback value %v, got %v", value, val)
	}
}

func TestSimpleCompositeStore_MockNATSClientErrors(t *testing.T) {
	logger := logging.Get()
	mockClient := NewMockNATSClient(logger)
	ctx := context.Background()

	// Test start error
	startErr := fmt.Errorf("start error")
	mockClient.SetErrors(startErr, nil, nil, nil)

	err := mockClient.Start(ctx)
	if err != startErr {
		t.Errorf("Expected start error %v, got %v", startErr, err)
	}

	// Reset errors and start successfully
	mockClient.SetErrors(nil, nil, nil, nil)
	err = mockClient.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Test get error
	getErr := fmt.Errorf("get error")
	mockClient.SetErrors(nil, getErr, nil, nil)

	_, err = mockClient.Get([]string{"test"})
	if err != getErr {
		t.Errorf("Expected get error %v, got %v", getErr, err)
	}

	// Test put error
	putErr := fmt.Errorf("put error")
	mockClient.SetErrors(nil, nil, putErr, nil)

	err = mockClient.Put([]string{"test"}, "value")
	if err != putErr {
		t.Errorf("Expected put error %v, got %v", putErr, err)
	}

	// Test delete error
	deleteErr := fmt.Errorf("delete error")
	mockClient.SetErrors(nil, nil, nil, deleteErr)

	err = mockClient.Delete([]string{"test"})
	if err != deleteErr {
		t.Errorf("Expected delete error %v, got %v", deleteErr, err)
	}
}

// Benchmark tests
func BenchmarkSimpleCompositeStore_PathRouting(b *testing.B) {
	// Create a test store with some regex patterns for realistic benchmarking
	regexPatterns := []string{
		"^data/schemas($|/.*)",
		"^data/resource_types($|/.*)",
		"^data/relationships($|/.*)",
		"^data/role_assignments($|/.*)",
	}
	store := createTestCompositeStore(regexPatterns)

	paths := []storage.Path{
		{"data", "schemas", "user"},
		{"data", "resource_types", "document"},
		{"data", "relationships", "member"},
		{"data", "role_assignments", "admin"},
		{"policy", "test"},
		{"system", "config"},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := paths[i%len(paths)]
		_ = store.shouldUseNATS(path)
	}
}

func BenchmarkSimpleCompositeStore_MockStoreOperations(b *testing.B) {
	mockStore := NewMockStore()
	ctx := context.Background()

	txn, err := mockStore.NewTransaction(ctx)
	if err != nil {
		b.Fatalf("Failed to create transaction: %v", err)
	}

	value := map[string]interface{}{"type": "test"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := storage.Path{"data", "test", fmt.Sprintf("key%d", i)}

		// Write
		err := mockStore.Write(ctx, txn, storage.AddOp, path, value)
		if err != nil {
			b.Errorf("Write error: %v", err)
		}

		// Read
		_, err = mockStore.Read(ctx, txn, path)
		if err != nil {
			b.Errorf("Read error: %v", err)
		}
	}
}

func BenchmarkSimpleCompositeStore_MockNATSClientOperations(b *testing.B) {
	logger := logging.Get()
	mockClient := NewMockNATSClient(logger)
	ctx := context.Background()

	err := mockClient.Start(ctx)
	if err != nil {
		b.Fatalf("Failed to start client: %v", err)
	}

	value := map[string]interface{}{"type": "test"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := []string{"data", "test", fmt.Sprintf("key%d", i)}

		// Put
		err := mockClient.Put(path, value)
		if err != nil {
			b.Errorf("Put error: %v", err)
		}

		// Get
		_, err = mockClient.Get(path)
		if err != nil {
			b.Errorf("Get error: %v", err)
		}
	}
}
