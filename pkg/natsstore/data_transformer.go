package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
)

// DataTransformer handles conversion between NATS keys and OPA store paths
type DataTransformer struct {
	rootBucket string
	logger     logging.Logger
}

// NewDataTransformer creates a new data transformer
func NewDataTransformer(config *Config, logger logging.Logger) (*DataTransformer, error) {
	dt := &DataTransformer{
		rootBucket: config.RootBucket,
		logger:     logger,
	}

	return dt, nil
}

// NATSKeyToOPAPath converts a NATS key to an OPA storage path
// For example: "users.alice.profile" -> ["data", "users", "alice", "profile"]
// If isRoot=true, path is /nats/kv/{keyParts...}
// If isRoot=false, path is /nats/kv/{bucketName}/{keyParts...}
func (dt *DataTransformer) NATSKeyToOPAPath(natsKey string, bucketName string, isRoot bool) (storage.Path, error) {
	if natsKey == "" {
		return nil, fmt.Errorf("empty NATS key")
	}

	// Split the NATS key by dots
	keyParts := strings.Split(natsKey, ".")

	// Create OPA path: ["data", "groupKey", ...keyParts]
	path := make(storage.Path, 0, len(keyParts)+2)
	// Add bucket name to path only if this is NOT root bucket data
	if !isRoot {
		path = append(path, "nats", "kv", bucketName)
	}

	path = append(path, keyParts...)

	return path, nil
}

// LoadGroupDataBulk loads all keys for a group from NATS and stores them in OPA store
func (dt *DataTransformer) LoadBucketDataBulk(ctx context.Context, bucketName string, natsClient *NATSClient, opaStore storage.Store, isRoot bool) (err error) {
	dt.logger.Debug("Loading bulk data for bucket: %s", bucketName)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in LoadBucketDataBulk: %v", r)
			dt.logger.Error("Panic in LoadBucketDataBulk: %v", r)
		}
	}()

	// Get the bucket for this group
	kv, err := natsClient.getBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}

	// List all keys in the bucket
	dt.logger.Debug("Getting keys from bucket %s", bucketName)
	keys, err := kv.Keys()
	if err != nil {
		// Handle the case where bucket is empty (NATS returns "no keys found" error)
		if err.Error() == "nats: no keys found" {
			dt.logger.Debug("Bucket %s is empty, no keys to load", bucketName)
			return nil
		}
		dt.logger.Error("Failed to list keys in bucket %s: %v", bucketName, err)
		return fmt.Errorf("failed to list keys in bucket %s: %w", bucketName, err)
	}
	dt.logger.Debug("Found %d keys in bucket %s", len(keys), bucketName)
	// Convert NATS keys to OPA base paths for the transaction
	basePaths := make([]string, 0)
	basePathSet := make(map[string]bool)
	for _, natsKey := range keys {
		opaPath, err := dt.NATSKeyToOPAPath(natsKey, bucketName, isRoot)
		if err != nil {
			dt.logger.Warn("Failed to convert NATS key to OPA path: %v", err)
			continue
		}
		// Use the base path (without the last element) for transaction
		if len(opaPath) > 1 {
			basePath := strings.Join(opaPath[:len(opaPath)-1], "/")
			if !basePathSet[basePath] {
				basePaths = append(basePaths, basePath)
				basePathSet[basePath] = true
			}
		}
	}
	// Create a transaction for bulk operations
	dt.logger.Debug("Creating transaction with base paths: %v", basePaths)
	txn, err := opaStore.NewTransaction(
		ctx,
		storage.TransactionParams{
			BasePaths: basePaths,
			Context:   storage.NewContext(),
			Write:     true,
		},
	)
	if err != nil {
		dt.logger.Error("Failed to create transaction: %v", err)
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	dt.logger.Debug("Transaction created successfully")
	successfulKeys := 0
	defer func(ctx context.Context, opaStore storage.Store, txn storage.Transaction) {
		if err != nil {
			opaStore.Abort(ctx, txn)
		} else {
			if err := opaStore.Commit(ctx, txn); err != nil {
				dt.logger.Error("failed to commit transaction: %v", err)
			} else if successfulKeys != len(keys) {
				dt.logger.Info("failed to load %d keys for bucket %s", len(keys)-successfulKeys, bucketName)
			} else {
				dt.logger.Info("Successfully loaded %d keys for bucket %s", len(keys), bucketName)
			}
		}
	}(ctx, opaStore, txn)

	// Load each key from NATS and store in OPA
	for _, natsKey := range keys {
		if err := dt.loadSingleKey(ctx, natsKey, bucketName, kv, opaStore, txn, isRoot); err != nil {
			dt.logger.Warn("Failed to load key '%s' for bucket '%s': %v", natsKey, bucketName, err)
		} else {
			successfulKeys += 1
		}
	}

	return nil
}

func (dt *DataTransformer) updateOPAStore(ctx context.Context, opaPath storage.Path, value any, opaStore storage.Store, txn storage.Transaction) error {
	// Store in OPA
	if value == nil {
		// For deletions, only try to remove if the path exists
		if err := opaStore.Write(ctx, txn, storage.RemoveOp, opaPath, value); err != nil {
			if storage.IsNotFound(err) {
				// Path doesn't exist, nothing to remove - this is OK
				dt.logger.Debug("Attempted to remove non-existent path %v, ignoring", opaPath)
				return nil
			}
			return fmt.Errorf("failed to remove from OPA store using RemoveOp: %w", err)
		}
	} else {
		// Try ReplaceOp first
		if err := opaStore.Write(ctx, txn, storage.ReplaceOp, opaPath, value); err != nil {
			dt.logger.Debug("failed to write to OPA store using ReplaceOp: %v", err)
			if storage.IsNotFound(err) {
				// Path doesn't exist, need to create parent paths first then use AddOp
				if err := dt.ensureParentPaths(ctx, opaPath, opaStore, txn); err != nil {
					return fmt.Errorf("failed to ensure parent paths: %w", err)
				}
				if err := opaStore.Write(ctx, txn, storage.AddOp, opaPath, value); err != nil {
					return fmt.Errorf("failed to add to OPA store using AddOp: %w", err)
				}
			} else {
				// ReplaceOp failed for other reasons (like type mismatch)
				// Try to remove the existing path and add the new value (upsert behavior)
				dt.logger.Debug("ReplaceOp failed, attempting upsert by removing then adding")
				if removeErr := opaStore.Write(ctx, txn, storage.RemoveOp, opaPath, nil); removeErr != nil {
					dt.logger.Debug("Failed to remove path during upsert: %v", removeErr)
				}
				if err := dt.ensureParentPaths(ctx, opaPath, opaStore, txn); err != nil {
					return fmt.Errorf("failed to ensure parent paths during upsert: %w", err)
				}
				if err := opaStore.Write(ctx, txn, storage.AddOp, opaPath, value); err != nil {
					return fmt.Errorf("failed to upsert to OPA store: %w", err)
				}
			}
		}
	}
	return nil
}

// ensureParentPaths creates all parent paths needed for the given path recursively
func (dt *DataTransformer) ensureParentPaths(ctx context.Context, opaPath storage.Path, opaStore storage.Store, txn storage.Transaction) error {
	return dt.ensureParentPathsRecursive(ctx, opaPath, len(opaPath)-1, opaStore, txn)
}

// ensureParentPathsRecursive works bottom-up to find the deepest missing parent and create the full structure
func (dt *DataTransformer) ensureParentPathsRecursive(ctx context.Context, fullPath storage.Path, currentDepth int, opaStore storage.Store, txn storage.Transaction) error {
	if currentDepth <= 0 {
		// We've reached the root, create the structure from here
		return dt.createNestedStructure(ctx, fullPath, 0, opaStore, txn)
	}

	parentPath := fullPath[:currentDepth]

	// Try to read the parent path to see if it exists
	_, err := opaStore.Read(ctx, txn, parentPath)
	if err == nil {
		// Parent exists, we can create the missing child structure from here
		return dt.createNestedStructure(ctx, fullPath, currentDepth, opaStore, txn)
	}

	if storage.IsNotFound(err) {
		// Parent doesn't exist, go deeper
		return dt.ensureParentPathsRecursive(ctx, fullPath, currentDepth-1, opaStore, txn)
	}

	// Some other error occurred
	return fmt.Errorf("failed to read parent path %v: %w", parentPath, err)
}

// createNestedStructure creates the nested object structure from the existing parent down to the target path
func (dt *DataTransformer) createNestedStructure(ctx context.Context, fullPath storage.Path, existingDepth int, opaStore storage.Store, txn storage.Transaction) error {
	if existingDepth >= len(fullPath)-1 {
		// Nothing to create
		return nil
	}

	// Build nested structure from existing depth to target
	var obj map[string]interface{}
	var valueToWrite interface{}
	var pathToCreate storage.Path

	if existingDepth == 0 {
		// Starting from root, create the entire structure
		obj = map[string]interface{}{}
		currentObj := obj

		// Create nested objects for each missing level
		for i := 0; i < len(fullPath)-1; i++ {
			key := string(fullPath[i])
			if i == len(fullPath)-2 {
				// This is the direct parent of our target, create empty object
				currentObj[key] = map[string]interface{}{}
			} else {
				// Intermediate level, create nested structure
				nextObj := map[string]interface{}{}
				currentObj[key] = nextObj
				currentObj = nextObj
			}
		}

		// Write the first level
		pathToCreate = fullPath[:1]
		valueToWrite = obj[string(fullPath[0])]
	} else {
		// Starting from an existing parent
		obj = map[string]interface{}{}
		currentObj := obj

		// Create nested objects for each missing level
		for i := existingDepth; i < len(fullPath)-1; i++ {
			key := string(fullPath[i])
			if i == len(fullPath)-2 {
				// This is the direct parent of our target, create empty object
				currentObj[key] = map[string]interface{}{}
			} else {
				// Intermediate level, create nested structure
				nextObj := map[string]interface{}{}
				currentObj[key] = nextObj
				currentObj = nextObj
			}
		}

		// Write the nested structure
		pathToCreate = fullPath[:existingDepth+1]
		valueToWrite = obj[string(fullPath[existingDepth])]
	}

	if err := opaStore.Write(ctx, txn, storage.AddOp, pathToCreate, valueToWrite); err != nil {
		if err.Error() != "storage_invalid_patch_error: path already exists" {
			return fmt.Errorf("failed to create nested structure at %v: %w", pathToCreate, err)
		}
	}

	dt.logger.Debug("Created nested structure from %v", pathToCreate)
	return nil
}

// loadSingleKey loads a single key from NATS KV and stores it in OPA store
func (dt *DataTransformer) loadSingleKey(ctx context.Context, natsKey string, bucketName string, kv nats.KeyValue, opaStore storage.Store, txn storage.Transaction, isRoot bool) error {
	// Get value from NATS
	entry, err := kv.Get(natsKey)
	if err != nil {
		return fmt.Errorf("failed to get key '%s' from NATS: %w", natsKey, err)
	}

	// Parse JSON value
	var value interface{}
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		// If JSON unmarshal fails, store as string
		value = string(entry.Value())
	}

	// Convert NATS key to OPA path
	opaPath, err := dt.NATSKeyToOPAPath(natsKey, bucketName, isRoot)
	if err != nil {
		return fmt.Errorf("failed to convert NATS key to OPA path: %w", err)
	}

	if err := dt.updateOPAStore(ctx, opaPath, value, opaStore, txn); err != nil {
		return fmt.Errorf("failed to update OPA store: %w", err)
	}

	dt.logger.Debug("Loaded key '%s' -> '%v' into OPA store", natsKey, opaPath)
	return nil
}

// InjectDataToOPAStore converts NATS data to OPA store writes (for watch updates)
func (dt *DataTransformer) InjectDataToOPAStore(ctx context.Context, opaStore storage.Store, bucketName string, natsKey string, value any, isRoot bool) error {
	// Convert NATS key to OPA path
	opaPath, err := dt.NATSKeyToOPAPath(natsKey, bucketName, isRoot)
	if err != nil {
		return fmt.Errorf("failed to convert NATS key to OPA path: %w", err)
	}

	// Create transaction
	txn, err := opaStore.NewTransaction(ctx, storage.TransactionParams{
		BasePaths: []string{bucketName},
		Context:   storage.NewContext(),
		Write:     true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer func(ctx context.Context, opaStore storage.Store, txn storage.Transaction) {
		if err != nil {
			opaStore.Abort(ctx, txn)
		} else {
			if err := opaStore.Commit(ctx, txn); err != nil {
				dt.logger.Error("failed to commit transaction: %v", err)
			} else {
				dt.logger.Debug("Injected '%s' -> '%v' into OPA store", natsKey, opaPath)
			}
		}
	}(ctx, opaStore, txn)

	if err := dt.updateOPAStore(ctx, opaPath, value, opaStore, txn); err != nil {
		return fmt.Errorf("failed to update OPA store: %w", err)
	}

	return nil
}
