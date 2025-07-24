package natsstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/open-policy-agent/opa/v1/logging"
)

// NATSCache provides group-based LRU caching on top of NATS K/V.
type NATSCache struct {
	config              *Config
	groupWatcherManager *GroupWatcherManager
	natsClient          *NATSClient
	logger              logging.Logger
	mu                  sync.RWMutex
	started             bool
}

// NewNATSCache creates a new NATS cache with group-based watcher management.
func NewNATSCache(config *Config, logger logging.Logger) (*NATSCache, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	if err := config.ValidateWithDefaults(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	nc := &NATSCache{
		config: config,
		logger: logger,
	}

	// Create NATS client without cache dependency (group watchers will manage their own caches)
	natsClient, err := NewNATSClient(config, nil, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS client: %w", err)
	}
	nc.natsClient = natsClient

	// Create group watcher manager if patterns or single group mode is configured
	if config.GroupRegexPattern != "" || config.SingleGroup != "" {
		groupWatcherManager, err := NewGroupWatcherManager(
			natsClient,
			config.MaxGroupWatchers,
			config.GroupWatcherCacheSize,
			logger,
			config,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create group watcher manager: %w", err)
		}
		nc.groupWatcherManager = groupWatcherManager
	}

	return nc, nil
}

// Start initializes the NATS connection and group watcher manager.
func (nc *NATSCache) Start(ctx context.Context) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if nc.started {
		return nil
	}

	nc.logger.Info("Starting NATS cache with group-based watching")

	// Start NATS client connection (but not the old global watcher)
	if err := nc.natsClient.connect(); err != nil {
		return fmt.Errorf("failed to connect NATS client: %w", err)
	}

	if nc.config.EnableBucketRouting {
		nc.logger.Info("NATS cache configured for bucket routing - each group will use its own bucket")
	} else {
		nc.logger.Info("NATS cache configured for single bucket with group watchers")
	}

	nc.started = true
	nc.logger.Info("NATS cache started successfully")
	return nil
}

// Stop shuts down the NATS cache and all group watchers.
func (nc *NATSCache) Stop(ctx context.Context) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if !nc.started {
		return nil
	}

	nc.logger.Info("Stopping NATS cache")

	// Stop group watcher manager if it exists
	if nc.groupWatcherManager != nil {
		if err := nc.groupWatcherManager.Stop(); err != nil {
			nc.logger.Error("Error stopping group watcher manager: %v", err)
		}
	}

	// Stop NATS client
	if nc.natsClient != nil {
		if nc.natsClient.conn != nil {
			nc.natsClient.conn.Close()
		}
	}

	nc.started = false
	nc.logger.Info("NATS cache stopped")
	return nil
}

// Get retrieves a value using group-based caching or falls back to direct NATS access.
func (nc *NATSCache) Get(ctx context.Context, path []string) (interface{}, bool, error) {
	nc.mu.RLock()
	started := nc.started
	nc.mu.RUnlock()

	if !started {
		return nil, false, fmt.Errorf("cache not started")
	}

	// If group watcher manager is available and can extract group, use it
	if nc.groupWatcherManager != nil {
		if _, hasGroup := nc.groupWatcherManager.ExtractGroup(path); hasGroup {
			value, found, err := nc.groupWatcherManager.Get(ctx, path)
			if err != nil {
				nc.logger.Debug("Group watcher manager failed, falling back to direct NATS: %v", err)
			} else {
				return value, found, nil
			}
		}
	}

	// Fallback to direct NATS K/V access for paths without groups
	value, err := nc.natsClient.Get(path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get from NATS: %w", err)
	}

	if value != nil {
		return value, true, nil
	}

	return nil, false, nil
}

// Set stores a value using group-based caching or falls back to direct NATS access.
func (nc *NATSCache) Set(ctx context.Context, path []string, value interface{}) error {
	nc.mu.RLock()
	defer nc.mu.RUnlock()

	if !nc.started {
		return fmt.Errorf("cache not started")
	}

	// If group watcher manager is available and can extract group, use it
	if nc.groupWatcherManager != nil {
		if _, hasGroup := nc.groupWatcherManager.ExtractGroup(path); hasGroup {
			err := nc.groupWatcherManager.Set(ctx, path, value)
			if err != nil {
				nc.logger.Debug("Group watcher manager failed, falling back to direct NATS: %v", err)
			} else {
				nc.logger.Debug("Stored value using group watcher for path: %v", path)
				return nil
			}
		}
	}

	// Fallback to direct NATS K/V access for paths without groups
	if err := nc.natsClient.Put(path, value); err != nil {
		return fmt.Errorf("failed to put to NATS: %w", err)
	}

	nc.logger.Debug("Stored value using direct NATS for path: %v", path)
	return nil
}

// Delete removes a value using group-based caching or falls back to direct NATS access.
func (nc *NATSCache) Delete(ctx context.Context, path []string) error {
	nc.mu.RLock()
	defer nc.mu.RUnlock()

	if !nc.started {
		return fmt.Errorf("cache not started")
	}

	// If group watcher manager is available and can extract group, use it
	if nc.groupWatcherManager != nil {
		if _, hasGroup := nc.groupWatcherManager.ExtractGroup(path); hasGroup {
			err := nc.groupWatcherManager.Delete(ctx, path)
			if err != nil {
				nc.logger.Debug("Group watcher manager failed, falling back to direct NATS: %v", err)
			} else {
				nc.logger.Debug("Deleted value using group watcher for path: %v", path)
				return nil
			}
		}
	}

	// Fallback to direct NATS K/V access for paths without groups
	if err := nc.natsClient.Delete(path); err != nil {
		return fmt.Errorf("failed to delete from NATS: %w", err)
	}

	nc.logger.Debug("Deleted value using direct NATS for path: %v", path)
	return nil
}

// Clear is not applicable for group-based caching since each group manages its own cache.
func (nc *NATSCache) Clear() {
	nc.mu.RLock()
	defer nc.mu.RUnlock()

	if nc.groupWatcherManager != nil {
		// We can't clear individual group caches from here
		// Each group watcher manages its own cache lifecycle
		nc.logger.Debug("Clear operation not supported for group-based caching")
	}
}

// GetStats returns cache statistics including group watcher information.
func (nc *NATSCache) GetStats() map[string]interface{} {
	nc.mu.RLock()
	defer nc.mu.RUnlock()

	stats := map[string]interface{}{
		"started":             nc.started,
		"group_based_caching": nc.groupWatcherManager != nil,
	}

	if nc.natsClient != nil {
		stats["nats_connected"] = nc.natsClient.isConnected()
	}

	if nc.groupWatcherManager != nil {
		groupStats := nc.groupWatcherManager.GetStats()
		for k, v := range groupStats {
			stats[k] = v
		}
	}

	return stats
}

// IsStarted returns whether the cache is started.
func (nc *NATSCache) IsStarted() bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.started
}
