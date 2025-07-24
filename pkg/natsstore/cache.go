package natsstore

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// EvictCallback is called when an entry is evicted from the cache.
type EvictCallback func(key string, value interface{})

// GroupCache implements a single JSON document with an LRU cache on top for accessed paths.
// This represents the entire NATS K/V bucket as one document.
type GroupCache struct {
	document      string                          // Single JSON document representing the entire bucket
	lruCache      *lru.Cache[string, interface{}] // LRU cache for recently accessed paths
	capacity      int
	mutex         sync.RWMutex
	logger        logging.Logger
	evictCallback EvictCallback
}

// NewGroupCache creates a new gjson-based cache with a single document and LRU cache on top.
func NewGroupCache(size int, logger logging.Logger) (*GroupCache, error) {
	sc := &GroupCache{
		document: "{}", // Start with empty JSON document
		capacity: size,
		logger:   logger,
	}

	// Create LRU cache with eviction callback
	cache, err := lru.NewWithEvict[string, interface{}](size, sc.onEvict)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}
	sc.lruCache = cache

	return sc, nil
}

// SetEvictCallback sets the eviction callback function.
func (sc *GroupCache) SetEvictCallback(callback EvictCallback) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.evictCallback = callback
}

// onEvict is called when an entry is evicted from the LRU cache.
func (sc *GroupCache) onEvict(key string, value interface{}) {
	if sc.evictCallback != nil {
		sc.evictCallback(key, value)
	}
}

// Get retrieves a value from the cache using gjson path syntax.
func (sc *GroupCache) Get(path []string) (interface{}, bool) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	if len(path) == 0 {
		// Return the entire document
		result := gjson.Parse(sc.document)
		if result.Exists() {
			return result.Value(), true
		}
		return nil, false
	}

	// Convert path to gjson path string
	gjsonPath := strings.Join(path, ".")

	// Check LRU cache first
	if cachedValue, found := sc.lruCache.Get(gjsonPath); found {
		sc.logger.Debug("LRU cache hit: path=%s", gjsonPath)
		return cachedValue, true
	}

	// Query the document using gjson
	result := gjson.Get(sc.document, gjsonPath)
	if result.Exists() {
		value := result.Value()
		// Cache the result in LRU (this will handle eviction automatically)
		sc.lruCache.Add(gjsonPath, value)
		sc.logger.Debug("Document hit, cached in LRU: path=%s", gjsonPath)
		return value, true
	}

	return nil, false
}

// GetByPrefix is maintained for backward compatibility but now just calls Get.
func (sc *GroupCache) GetByPrefix(prefix []string) (map[string]interface{}, bool) {
	result, found := sc.Get(prefix)
	if !found {
		return nil, false
	}

	// If result is already a map, return it
	if resultMap, ok := result.(map[string]interface{}); ok {
		return resultMap, true
	}

	// If it's a single value, this isn't really a "prefix" query
	return nil, false
}

// Put stores a value at the specified path and invalidates affected cache entries.
func (sc *GroupCache) Put(path []string, value interface{}) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if len(path) == 0 {
		// Replace the entire document
		valueJSON, err := json.Marshal(value)
		if err != nil {
			sc.logger.Error("Failed to marshal value for document replacement: %v", err)
			return
		}
		sc.document = string(valueJSON)
		// Clear the entire LRU cache since the whole document changed
		sc.lruCache.Purge()
		sc.logger.Debug("Replaced entire document and purged LRU cache")
		return
	}

	// Convert value to JSON for sjson
	valueJSON, err := json.Marshal(value)
	if err != nil {
		sc.logger.Error("Failed to marshal value for caching: %v", err)
		return
	}

	// Convert path to gjson path string
	gjsonPath := strings.Join(path, ".")

	// Update the document using sjson
	updatedDoc, err := sjson.Set(sc.document, gjsonPath, json.RawMessage(valueJSON))
	if err != nil {
		sc.logger.Error("Failed to set value in document: %v", err)
		return
	}

	sc.document = updatedDoc

	// Invalidate affected cache entries
	sc.invalidateAffectedPaths(path)

	sc.logger.Debug("Updated document and invalidated cache for path: %s", gjsonPath)
}

// Remove removes a value from the document and invalidates affected cache entries.
func (sc *GroupCache) Remove(path []string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if len(path) == 0 {
		// Clear the entire document
		sc.document = "{}"
		sc.lruCache.Purge()
		sc.logger.Debug("Cleared entire document and purged LRU cache")
		return
	}

	// Convert path to gjson path string
	gjsonPath := strings.Join(path, ".")

	// Remove from document using sjson
	updatedDoc, err := sjson.Delete(sc.document, gjsonPath)
	if err != nil {
		sc.logger.Error("Failed to delete path from document: %v", err)
		return
	}

	sc.document = updatedDoc

	// Invalidate affected cache entries
	sc.invalidateAffectedPaths(path)

	sc.logger.Debug("Removed from document and invalidated cache for path: %s", gjsonPath)
}

// invalidateAffectedPaths removes cache entries that are affected by changes to the given path.
// When x.y changes, we need to invalidate both x.y and x (since x now returns different data).
func (sc *GroupCache) invalidateAffectedPaths(changedPath []string) {
	pathsToInvalidate := make([]string, 0)

	// Invalidate the exact path and all parent paths
	for i := len(changedPath); i > 0; i-- {
		pathPrefix := changedPath[:i]
		gjsonPath := strings.Join(pathPrefix, ".")
		pathsToInvalidate = append(pathsToInvalidate, gjsonPath)
	}

	// Also check all cached keys to see if any are children of the changed path
	// (though this is less common since we usually change leaf nodes)
	changedGjsonPath := strings.Join(changedPath, ".")
	for _, cachedKey := range sc.lruCache.Keys() {
		if strings.HasPrefix(cachedKey, changedGjsonPath+".") {
			pathsToInvalidate = append(pathsToInvalidate, cachedKey)
		}
	}

	// Remove all affected paths from LRU cache
	for _, pathToInvalidate := range pathsToInvalidate {
		if sc.lruCache.Contains(pathToInvalidate) {
			sc.lruCache.Remove(pathToInvalidate)
			sc.logger.Debug("Invalidated LRU cache entry: %s", pathToInvalidate)
		}
	}
}

// RemoveByPrefix removes all entries that start with the given prefix.
func (sc *GroupCache) RemoveByPrefix(prefix []string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if len(prefix) == 0 {
		// Clear everything
		sc.document = "{}"
		sc.lruCache.Purge()
		sc.logger.Debug("Cleared entire document and purged LRU cache (prefix removal)")
		return
	}

	// For prefix removal, we need to examine the document and remove matching paths
	// This is more complex with a single document approach, but we can use gjson to find matches
	gjsonPrefix := strings.Join(prefix, ".")

	// Parse the current document to find all paths that start with the prefix
	result := gjson.Parse(sc.document)
	pathsToRemove := make([]string, 0)

	// Walk the JSON structure to find matching paths
	sc.findPathsWithPrefix(result, "", gjsonPrefix, &pathsToRemove)

	// Remove each matching path
	updatedDoc := sc.document
	for _, pathToRemove := range pathsToRemove {
		var err error
		updatedDoc, err = sjson.Delete(updatedDoc, pathToRemove)
		if err != nil {
			sc.logger.Error("Failed to delete path %s: %v", pathToRemove, err)
			continue
		}
	}

	sc.document = updatedDoc

	// Invalidate affected cache entries
	sc.invalidateAffectedPaths(prefix)

	sc.logger.Debug("Removed %d paths with prefix %s", len(pathsToRemove), gjsonPrefix)
}

// findPathsWithPrefix recursively finds all paths in the JSON that start with the given prefix.
func (sc *GroupCache) findPathsWithPrefix(value gjson.Result, currentPath, targetPrefix string, results *[]string) {
	if strings.HasPrefix(currentPath, targetPrefix) && (currentPath == targetPrefix || strings.HasPrefix(currentPath, targetPrefix+".")) {
		*results = append(*results, currentPath)
		return // Don't recurse further if we found a match
	}

	if value.IsObject() {
		value.ForEach(func(key, val gjson.Result) bool {
			newPath := currentPath
			if newPath != "" {
				newPath += "."
			}
			newPath += key.String()
			sc.findPathsWithPrefix(val, newPath, targetPrefix, results)
			return true
		})
	}
}

// Clear removes all entries from the cache.
func (sc *GroupCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.document = "{}"
	sc.lruCache.Purge()
	sc.logger.Debug("Cleared document and purged LRU cache")
}

// Size returns the number of entries in the LRU cache (not the document size).
func (sc *GroupCache) Size() int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return sc.lruCache.Len()
}

// Keys returns all cached keys in the LRU cache.
func (sc *GroupCache) Keys() []string {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return sc.lruCache.Keys()
}

// GetStats returns cache statistics.
func (sc *GroupCache) GetStats() map[string]interface{} {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	// Calculate document size
	documentSize := len(sc.document)

	return map[string]interface{}{
		"cache_size":      sc.lruCache.Len(),
		"cache_capacity":  sc.capacity,
		"cache_type":      "gjson_single_document_with_lru",
		"cache_ttl":       "N/A",
		"document_size":   documentSize,
		"document_exists": sc.document != "{}",
	}
}

// GetDocument returns the current document (for debugging/inspection).
func (sc *GroupCache) GetDocument() string {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	return sc.document
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
