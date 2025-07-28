package natsstore

import (
	"context"
	"fmt"

	"github.com/open-policy-agent/opa/v1/logging"
	"github.com/open-policy-agent/opa/v1/storage"
)


// BucketDataManager manages group data loading and injection into OPA store
type BucketDataManager struct {
	natsClient      *NATSClient
	watcherManager  *BucketWatcherManager
	dataTransformer *DataTransformer
	logger          logging.Logger
	config          *Config
	started         bool
}

// NewBucketDataManager creates a new group data manager
func NewBucketDataManager(config *Config, logger logging.Logger) (*BucketDataManager, error) {
	// Create NATS client
	natsClient, err := NewNATSClient(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS client: %w", err)
	}

	// Create group watcher manager
	watcherManager, err := NewBucketWatcherManager(natsClient, config.MaxBucketsWatchers, logger, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create group watcher manager: %w", err)
	}

	// Create data transformer
	dataTransformer, err := NewDataTransformer(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create data transformer: %w", err)
	}

	return &BucketDataManager{
		natsClient:      natsClient,
		watcherManager:  watcherManager,
		dataTransformer: dataTransformer,
		logger:          logger,
		config:          config,
	}, nil
}

// Start initializes the group data manager
func (gdm *BucketDataManager) Start(ctx context.Context) error {
	if gdm.started {
		return nil
	}

	// NATS client is already connected in NewNATSClient
	gdm.started = true
	gdm.logger.Info("Bucket data manager started successfully")
	return nil
}

// Stop shuts down the group data manager
func (gdm *BucketDataManager) Stop(ctx context.Context) error {
	if !gdm.started {
		return nil
	}

	if err := gdm.watcherManager.Stop(); err != nil {
		gdm.logger.Error("Error stopping group watcher manager: %v", err)
	}

	// NATS client doesn't have Stop method - connection is managed via config
	gdm.started = false
	gdm.logger.Info("Group data manager stopped")
	return nil
}

// EnsureGroupLoaded ensures a group is watched and all data is loaded into OPA store
func (gdm *BucketDataManager) EnsureBucketLoaded(ctx context.Context, bucketName string, opaStore storage.Store) error {
	gdm.logger.Debug("Ensuring bucket %s is loaded into OPA store", bucketName)

	if gdm.watcherManager.HasWatcher(bucketName) {
		gdm.logger.Debug("Bucket %s already has a watcher", bucketName)
		// this means that the bucket is already loaded into OPA store
		// and we don't need to do anything more than that
		return nil
	}

	// Load bucket data from NATS into OPA store synchronously
	if err := gdm.dataTransformer.LoadBucketDataBulk(ctx, bucketName, gdm.natsClient, opaStore); err != nil {
		return fmt.Errorf("failed to load bucket data for %s: %w", bucketName, err)
	}

	// Start watching for future changes (async)
	if _, err := gdm.watcherManager.GetOrCreateWatcher(bucketName, opaStore); err != nil {
		return err
	}

	gdm.logger.Info("Bucket %s successfully loaded into OPA store", bucketName)
	return nil
}
