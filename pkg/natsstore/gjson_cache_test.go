package natsstore

import (
	"testing"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGjsonGroupCache_SingleDocumentWithLRU(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(5, logger)
	require.NoError(t, err)

	// Store individual values - they all go into the single document
	cache.Put([]string{"metadata", "x"}, "value_x")
	cache.Put([]string{"metadata", "y"}, "value_y")
	cache.Put([]string{"user", "profile", "name"}, "John Doe")

	// Verify the document structure
	doc := cache.GetDocument()
	assert.Contains(t, doc, "metadata")
	assert.Contains(t, doc, "user")

	// First access should hit the document and cache the result
	x, found := cache.Get([]string{"metadata", "x"})
	assert.True(t, found)
	assert.Equal(t, "value_x", x)

	// Second access should hit the LRU cache
	x2, found := cache.Get([]string{"metadata", "x"})
	assert.True(t, found)
	assert.Equal(t, "value_x", x2)

	// Check that it's actually cached in LRU
	assert.Equal(t, 1, cache.Size()) // One entry in LRU cache

	// Access parent path - should hit document and cache
	metadata, found := cache.Get([]string{"metadata"})
	assert.True(t, found)
	metadataMap, ok := metadata.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "value_x", metadataMap["x"])
	assert.Equal(t, "value_y", metadataMap["y"])

	// Now we should have 2 entries in LRU cache
	assert.Equal(t, 2, cache.Size())
}

func TestGjsonGroupCache_CacheInvalidation(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(10, logger)
	require.NoError(t, err)

	// Store initial values
	cache.Put([]string{"user", "profile", "name"}, "John Doe")
	cache.Put([]string{"user", "profile", "age"}, 30)
	cache.Put([]string{"user", "settings", "theme"}, "dark")

	// Access various paths to populate LRU cache
	_, _ = cache.Get([]string{"user", "profile", "name"}) // Cache: user.profile.name
	_, _ = cache.Get([]string{"user", "profile"})         // Cache: user.profile.name, user.profile
	_, _ = cache.Get([]string{"user"})                    // Cache: user.profile.name, user.profile, user

	assert.Equal(t, 3, cache.Size()) // 3 entries in LRU

	// Now change user.profile.name - this should invalidate:
	// - user.profile.name (exact path)
	// - user.profile (parent path)
	// - user (parent path)
	cache.Put([]string{"user", "profile", "name"}, "Jane Smith")

	// LRU cache should be cleared for affected paths
	assert.Equal(t, 0, cache.Size()) // All affected entries invalidated

	// Verify the document was updated correctly
	name, found := cache.Get([]string{"user", "profile", "name"})
	assert.True(t, found)
	assert.Equal(t, "Jane Smith", name)

	// Verify parent path reflects the change
	profile, found := cache.Get([]string{"user", "profile"})
	assert.True(t, found)
	profileMap, ok := profile.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "Jane Smith", profileMap["name"])
}

func TestGjsonGroupCache_DocumentPersistence(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(3, logger) // Small cache to test eviction
	require.NoError(t, err)

	// Store several values
	cache.Put([]string{"a"}, "value_a")
	cache.Put([]string{"b"}, "value_b")
	cache.Put([]string{"c"}, "value_c")
	cache.Put([]string{"d"}, "value_d")

	// Access all values to fill LRU cache beyond capacity
	_, _ = cache.Get([]string{"a"})
	_, _ = cache.Get([]string{"b"})
	_, _ = cache.Get([]string{"c"})
	_, _ = cache.Get([]string{"d"})

	// LRU should only hold 3 entries (cache capacity)
	assert.Equal(t, 3, cache.Size())

	// But all values should still be accessible from the document
	a, found := cache.Get([]string{"a"})
	assert.True(t, found)
	assert.Equal(t, "value_a", a)

	d, found := cache.Get([]string{"d"})
	assert.True(t, found)
	assert.Equal(t, "value_d", d)

	// Document should contain all values
	doc := cache.GetDocument()
	assert.Contains(t, doc, "value_a")
	assert.Contains(t, doc, "value_b")
	assert.Contains(t, doc, "value_c")
	assert.Contains(t, doc, "value_d")
}

func TestGjsonGroupCache_ComplexNesting(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(10, logger)
	require.NoError(t, err)

	// Store complex nested structure
	complexData := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
			map[string]interface{}{
				"id":   2,
				"name": "Bob",
			},
		},
		"settings": map[string]interface{}{
			"theme": "dark",
		},
	}

	cache.Put([]string{"app", "data"}, complexData)

	// Access nested paths
	users, found := cache.Get([]string{"app", "data", "users"})
	assert.True(t, found)
	usersList, ok := users.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(usersList))

	settings, found := cache.Get([]string{"app", "data", "settings"})
	assert.True(t, found)
	settingsMap, ok := settings.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "dark", settingsMap["theme"])

	// Modify a nested value and verify cache invalidation
	cache.Put([]string{"app", "data", "settings", "theme"}, "light")

	// Previous cache entry for "app.data.settings" should be invalidated
	newSettings, found := cache.Get([]string{"app", "data", "settings"})
	assert.True(t, found)
	newSettingsMap, ok := newSettings.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "light", newSettingsMap["theme"])
}

func TestGjsonGroupCache_RemovalAndInvalidation(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(10, logger)
	require.NoError(t, err)

	// Store hierarchical data
	cache.Put([]string{"user", "profile", "name"}, "John")
	cache.Put([]string{"user", "profile", "age"}, 30)
	cache.Put([]string{"user", "settings", "theme"}, "dark")

	// Populate LRU cache
	_, _ = cache.Get([]string{"user", "profile", "name"})
	_, _ = cache.Get([]string{"user", "profile"})
	_, _ = cache.Get([]string{"user"})

	assert.Equal(t, 3, cache.Size())

	// Remove a specific field
	cache.Remove([]string{"user", "profile", "name"})

	// Cache should be invalidated for affected paths
	assert.Equal(t, 0, cache.Size())

	// Field should be gone from document
	_, found := cache.Get([]string{"user", "profile", "name"})
	assert.False(t, found)

	// But other fields should still exist
	age, found := cache.Get([]string{"user", "profile", "age"})
	assert.True(t, found)
	assert.Equal(t, float64(30), age)

	theme, found := cache.Get([]string{"user", "settings", "theme"})
	assert.True(t, found)
	assert.Equal(t, "dark", theme)
}

func TestGjsonGroupCache_EvictionCallback(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(2, logger) // Small cache
	require.NoError(t, err)

	evictedKeys := make([]string, 0)
	cache.SetEvictCallback(func(key string, value interface{}) {
		evictedKeys = append(evictedKeys, key)
	})

	// Fill cache beyond capacity
	cache.Put([]string{"a"}, "value_a")
	cache.Put([]string{"b"}, "value_b")
	cache.Put([]string{"c"}, "value_c")

	// Access to trigger eviction
	_, _ = cache.Get([]string{"a"})
	_, _ = cache.Get([]string{"b"})
	_, _ = cache.Get([]string{"c"}) // This should cause eviction

	// Callback should have been called
	assert.True(t, len(evictedKeys) > 0)
}

func TestGjsonGroupCache_BackwardCompatibility(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(10, logger)
	require.NoError(t, err)

	// Store data
	cache.Put([]string{"permissions", "read"}, true)
	cache.Put([]string{"permissions", "write"}, false)

	// GetByPrefix should work for map results
	perms, found := cache.GetByPrefix([]string{"permissions"})
	assert.True(t, found)
	assert.Equal(t, true, perms["read"])
	assert.Equal(t, false, perms["write"])

	// GetByPrefix should return false for non-map results
	_, found = cache.GetByPrefix([]string{"permissions", "read"})
	assert.False(t, found)
}

func TestGjsonGroupCache_Stats(t *testing.T) {
	logger := logging.New()
	cache, err := NewGroupCache(5, logger)
	require.NoError(t, err)

	stats := cache.GetStats()
	assert.Equal(t, 0, stats["cache_size"])
	assert.Equal(t, 5, stats["cache_capacity"])
	assert.Equal(t, "gjson_single_document_with_lru", stats["cache_type"])
	assert.Equal(t, false, stats["document_exists"])

	// Add some data
	cache.Put([]string{"test"}, "value")
	_, _ = cache.Get([]string{"test"}) // Access to cache it

	stats = cache.GetStats()
	assert.Equal(t, 1, stats["cache_size"])
	assert.Equal(t, true, stats["document_exists"])
	assert.True(t, stats["document_size"].(int) > 0)

	cache.Clear()
	assert.Equal(t, 0, cache.Size())
	stats = cache.GetStats()
	assert.Equal(t, false, stats["document_exists"])
}
