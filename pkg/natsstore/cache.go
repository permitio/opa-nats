package natsstore

import (
	"encoding/json"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/logging"
)

// CacheEntry represents a cached schema entry with TTL.
type CacheEntry struct {
	Value     interface{}
	Path      []string
}

// EvictCallback is called when an entry is evicted from the cache.
type EvictCallback func(key string, entry *CacheEntry)

// GroupCache implements an LRU cache for schema data with TTL support.
type GroupCache struct {
	cache         *lru.Cache[string, *CacheEntry]
	capacity      int
	mutex         sync.RWMutex
	logger        logging.Logger
}

// NewGroupCache creates a new schema cache with the specified size and TTL.
func NewGroupCache(size int, logger logging.Logger) (*GroupCache, error) {
	
	cache, err := lru.New[string, *CacheEntry](size)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	sc := &GroupCache{
		cache:         cache,
		capacity:      size,
		logger:        logger,
	}

	return sc, nil
}

// Get retrieves a value from the cache by path.
func (sc *GroupCache) Get(path []string) (interface{}, bool) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	key := sc.pathToKey(path)
	entry, exists := sc.cache.Get(key)
	if !exists {
		return nil, false
	}

	return entry.Value, true
}

// Put stores a value in the cache with the specified path.
func (sc *GroupCache) Put(path []string, value interface{}) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	key := sc.pathToKey(path)
	entry := &CacheEntry{
		Value:     value,
		Path:      path,
	}

	evicted := sc.cache.Add(key, entry)
	if evicted {
		sc.logger.Debug("Cache entry evicted for path: %v", path)
	}
}

// Remove removes a value from the cache by path.
func (sc *GroupCache) Remove(path []string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	key := sc.pathToKey(path)
	sc.cache.Remove(key)
}

// RemoveByPrefix removes all cache entries that start with the given prefix.
func (sc *GroupCache) RemoveByPrefix(prefix []string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	keysToRemove := make([]string, 0)

	// Find all keys that start with the prefix
	for _, key := range sc.cache.Keys() {
		var keyPath []string
		if err := json.Unmarshal([]byte(key), &keyPath); err != nil {
			continue
		}
		if len(keyPath) >= len(prefix) {
			match := true
			for i := range prefix {
				if keyPath[i] != prefix[i] {
					match = false
					break
				}
			}
			if match {
				keysToRemove = append(keysToRemove, key)
			}
		}
	}

	// Remove the keys
	for _, key := range keysToRemove {
		sc.cache.Remove(key)
	}

	sc.logger.Debug("Removed %d cache entries with prefix: %v", len(keysToRemove), prefix)
}

// Clear removes all entries from the cache.
func (sc *GroupCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.cache.Purge()
	sc.logger.Debug("Cache cleared")
}

// Size returns the number of entries in the cache.
func (sc *GroupCache) Size() int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return sc.cache.Len()
}

// Keys returns all cache keys.
func (sc *GroupCache) Keys() []string {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return sc.cache.Keys()
}

// GetStats returns cache statistics.
func (sc *GroupCache) GetStats() map[string]interface{} {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return map[string]interface{}{
		"cache_size":     sc.cache.Len(),
		"cache_capacity": sc.capacity,
	}
}

// pathToKey converts a path slice to a string key.
func (sc *GroupCache) pathToKey(path []string) string {
	if len(path) == 0 {
		return ""
	}
	pathBytes, _ := json.Marshal(path)
	return string(pathBytes)
}


// ConvertToOPAValue converts a cached value to an OPA AST value.
func ConvertToOPAValue(value interface{}) (ast.Value, error) {
	switch v := value.(type) {
	case ast.Value:
		return v, nil
	case map[string]interface{}:
		return ast.InterfaceToValue(v)
	case []interface{}:
		return ast.InterfaceToValue(v)
	case string:
		return ast.String(v), nil
	case int:
		return ast.Number(json.Number(fmt.Sprintf("%d", v))), nil
	case float64:
		return ast.Number(json.Number(fmt.Sprintf("%f", v))), nil
	case bool:
		return ast.Boolean(v), nil
	case nil:
		return ast.Null{}, nil
	default:
		// Try to convert through JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value to JSON: %w", err)
		}

		var jsonValue interface{}
		if err := json.Unmarshal(jsonBytes, &jsonValue); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON value: %w", err)
		}

		return ast.InterfaceToValue(jsonValue)
	}
}
