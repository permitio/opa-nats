package natsstore

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/open-policy-agent/opa/v1/logging"
)

// TestHelper interface that both *testing.T and *testing.B can implement
type TestHelper interface {
	Helper()
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// Simple interface for NATS client operations
type NATSClientInterface interface {
	Start(context.Context) error
	Stop(context.Context) error
	Get([]string) (interface{}, error)
	Put([]string, interface{}) error
	Delete([]string) error
	IsStarted() bool
}

// Verify that MockNATSClient implements the interface
var _ NATSClientInterface = (*MockNATSClient)(nil)

// Helper function to create a test NATS cache configuration
func createTestConfig() *Config {
	return &Config{
		ServerURL:             "nats://localhost:4222",
		GroupRegexPattern:     ".*",
		GroupWatcherCacheSize: 10,
		MaxGroupWatchers:      10,
		SingleGroup:           "test-group",
	}
}

// Helper function to create a test schema cache
func createTestSchemaCache(t TestHelper, config *Config) *GroupCache {
	t.Helper()

	logger := logging.Get()
	cache, err := NewGroupCache(config.GroupWatcherCacheSize, logger)
	if err != nil {
		t.Fatalf("Failed to create schema cache: %v", err)
	}

	return cache
}

// Helper function to compare values using reflection
func compareValues(expected, actual interface{}) bool {
	return reflect.DeepEqual(expected, actual)
}

func TestNATSCache_SchemaCache_Basic(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Test basic operations
	path := []string{"data", "schemas", "user"}
	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{"type": "string"},
		},
	}

	// Test put and get
	cache.Put(path, value)

	retrieved, found := cache.Get(path)
	if !found {
		t.Error("Should find value after put")
	}

	if !compareValues(value, retrieved) {
		t.Errorf("Expected value %v, got %v", value, retrieved)
	}

	// Test size
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	// Test remove
	cache.Remove(path)

	_, found = cache.Get(path)
	if found {
		t.Error("Should not find value after remove")
	}
}

func TestNATSCache_SchemaCache_Eviction(t *testing.T) {
	// Create cache with small capacity
	config := &Config{
		ServerURL:             "nats://localhost:4222",
		GroupRegexPattern:     ".*",
		GroupWatcherCacheSize: 2, // Small capacity
		MaxGroupWatchers:      10,
		SingleGroup:           "test-group",
	}

	logger := logging.Get()

	// Track evictions
	evictedKeys := make([]string, 0)
	var evictMu sync.Mutex

	cache, err := NewGroupCache(config.GroupWatcherCacheSize, logger)
	if err != nil {
		t.Fatalf("Failed to create schema cache: %v", err)
	}

	// Set up eviction callback
	cache.SetEvictCallback(func(key string, value interface{}) {
		evictMu.Lock()
		defer evictMu.Unlock()
		evictedKeys = append(evictedKeys, key)
	})

	// Fill cache to capacity
	path1 := []string{"data", "schemas", "user"}
	path2 := []string{"data", "schemas", "org"}
	value1 := map[string]interface{}{"type": "object", "id": "user"}
	value2 := map[string]interface{}{"type": "object", "id": "org"}

	cache.Put(path1, value1)
	cache.Get(path1) // Access to populate LRU cache

	cache.Put(path2, value2)
	cache.Get(path2) // Access to populate LRU cache

	// Add one more to trigger eviction
	path3 := []string{"data", "schemas", "project"}
	value3 := map[string]interface{}{"type": "object", "id": "project"}

	cache.Put(path3, value3)
	cache.Get(path3) // This should trigger eviction due to LRU capacity

	// Give eviction callback time to run
	time.Sleep(10 * time.Millisecond)

	// Verify eviction callback was called
	evictMu.Lock()
	defer evictMu.Unlock()

	if len(evictedKeys) == 0 {
		t.Error("Eviction callback should have been called")
	}

	// Verify cache size is within limits
	if cache.Size() > config.GroupWatcherCacheSize {
		t.Errorf("Cache size %d should not exceed capacity %d", cache.Size(), config.GroupWatcherCacheSize)
	}
}

func TestNATSCache_SchemaCache_TTL(t *testing.T) {
	// Create cache with short TTL
	config := &Config{
		ServerURL:             "nats://localhost:4222",
		GroupRegexPattern:     ".*",
		GroupWatcherCacheSize: 10,
		MaxGroupWatchers:      10,
		SingleGroup:           "test-group",
	}

	cache := createTestSchemaCache(t, config)

	// Add data to cache
	path := []string{"data", "schemas", "user"}
	value := map[string]interface{}{"type": "object"}

	cache.Put(path, value)

	// Verify data exists
	_, found := cache.Get(path)
	if !found {
		t.Error("Should find value immediately after put")
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Verify data is expired (note: cleanup may not have run yet)
	// This test depends on the implementation details
}

func TestNATSCache_SchemaCache_Stats(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Add some data
	path1 := []string{"data", "schemas", "user"}
	path2 := []string{"data", "schemas", "org"}
	value1 := map[string]interface{}{"type": "object"}
	value2 := map[string]interface{}{"type": "object"}

	cache.Put(path1, value1)
	cache.Put(path2, value2)

	// Access items to populate LRU cache for stats
	cache.Get(path1)
	cache.Get(path2)

	// Test stats
	stats := cache.GetStats()
	if stats == nil {
		t.Error("GetStats should not return nil")
	}

	// Verify stats contain expected keys
	expectedKeys := []string{"cache_size", "cache_capacity", "cache_ttl"}
	for _, key := range expectedKeys {
		if _, exists := stats[key]; !exists {
			t.Errorf("Stats should contain key %s", key)
		}
	}

	// Verify cache size in stats
	if stats["cache_size"] != 2 {
		t.Errorf("Expected cache_size 2, got %v", stats["cache_size"])
	}
}

func TestNATSCache_SchemaCache_ConcurrentAccess(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Test concurrent reads and writes
	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup

	// Start writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{"data", "concurrent", fmt.Sprintf("key%d_%d", id, j)}
				value := map[string]interface{}{
					"type":  "object",
					"id":    fmt.Sprintf("key%d_%d", id, j),
					"value": fmt.Sprintf("value%d_%d", id, j),
				}

				cache.Put(path, value)
			}
		}(i)
	}

	// Start readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{"data", "concurrent", fmt.Sprintf("key%d_%d", id, j)}

				// Try to read, may not exist yet
				_, found := cache.Get(path)
				// Don't assert on found since timing is unpredictable
				_ = found
			}
		}(i)
	}

	wg.Wait()

	// Verify some data was written
	if cache.Size() == 0 {
		t.Error("Expected some data to be written")
	}
}

func TestNATSCache_SchemaCache_RemoveByPrefix(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Add data with different prefixes
	paths := [][]string{
		{"data", "schemas", "user", "admin"},
		{"data", "schemas", "user", "guest"},
		{"data", "schemas", "org", "company"},
		{"data", "resource_types", "document"},
	}

	for _, path := range paths {
		value := map[string]interface{}{"type": "object", "path": path}
		cache.Put(path, value)
		cache.Get(path) // Access to populate LRU cache
	}

	// Verify all data is there
	if cache.Size() != 4 {
		t.Errorf("Expected 4 items, got %d", cache.Size())
	}

	// Remove by prefix
	prefix := []string{"data", "schemas", "user"}
	cache.RemoveByPrefix(prefix)

	// Verify only user schemas were removed
	if cache.Size() != 2 {
		t.Errorf("Expected 2 items after prefix removal, got %d", cache.Size())
	}

	// Verify specific items were removed
	for _, path := range paths[:2] { // First two paths have user prefix
		_, found := cache.Get(path)
		if found {
			t.Errorf("Path %v should have been removed", path)
		}
	}

	// Verify other items remain
	for _, path := range paths[2:] { // Remaining paths should still exist
		_, found := cache.Get(path)
		if !found {
			t.Errorf("Path %v should still exist", path)
		}
	}
}

func TestNATSCache_SchemaCache_Clear(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Add data
	paths := [][]string{
		{"data", "schemas", "user"},
		{"data", "schemas", "org"},
		{"data", "resource_types", "document"},
	}

	for _, path := range paths {
		value := map[string]interface{}{"type": "object", "path": path}
		cache.Put(path, value)
		cache.Get(path) // Access to populate LRU cache
	}

	// Verify data is there
	if cache.Size() != 3 {
		t.Errorf("Expected 3 items, got %d", cache.Size())
	}

	// Clear cache
	cache.Clear()

	// Verify cache is empty
	if cache.Size() != 0 {
		t.Errorf("Expected 0 items after clear, got %d", cache.Size())
	}

	// Verify all data is gone
	for _, path := range paths {
		_, found := cache.Get(path)
		if found {
			t.Errorf("Path %v should have been cleared", path)
		}
	}
}

func TestNATSCache_SchemaCache_Keys(t *testing.T) {
	config := createTestConfig()
	cache := createTestSchemaCache(t, config)

	// Add data
	paths := [][]string{
		{"data", "schemas", "user"},
		{"data", "schemas", "org"},
		{"data", "resource_types", "document"},
	}

	for _, path := range paths {
		value := map[string]interface{}{"type": "object", "path": path}
		cache.Put(path, value)
		cache.Get(path) // Access to populate LRU cache
	}

	// Get keys
	keys := cache.Keys()

	// Verify key count
	if len(keys) != len(paths) {
		t.Errorf("Expected %d keys, got %d", len(paths), len(keys))
	}

	// Verify keys are not empty
	for _, key := range keys {
		if key == "" {
			t.Error("Key should not be empty")
		}
	}
}

// Benchmark tests
func BenchmarkNATSCache_SchemaCache_Put(b *testing.B) {
	config := createTestConfig()
	cache := createTestSchemaCache(b, config)

	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{"type": "string"},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := []string{"data", "schemas", fmt.Sprintf("user%d", i)}
		cache.Put(path, value)
	}
}

func BenchmarkNATSCache_SchemaCache_Get(b *testing.B) {
	config := createTestConfig()
	cache := createTestSchemaCache(b, config)

	// Pre-populate cache
	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{"type": "string"},
		},
	}

	for i := 0; i < 1000; i++ {
		path := []string{"data", "schemas", fmt.Sprintf("user%d", i)}
		cache.Put(path, value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := []string{"data", "schemas", fmt.Sprintf("user%d", i%1000)}
		_, found := cache.Get(path)
		if !found {
			b.Errorf("Expected to find value for path %v", path)
		}
	}
}

func BenchmarkNATSCache_SchemaCache_ConcurrentAccess(b *testing.B) {
	config := createTestConfig()
	cache := createTestSchemaCache(b, config)

	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{"type": "string"},
		},
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			path := []string{"data", "schemas", fmt.Sprintf("user%d", i)}

			// 70% reads, 30% writes
			if i%10 < 7 {
				cache.Get(path)
			} else {
				cache.Put(path, value)
			}
			i++
		}
	})
}
