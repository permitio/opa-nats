package natsstore

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
)

// NATSClient manages the connection to NATS and K/V operations.
type NATSClient struct {
	config    *Config
	conn      *nats.Conn
	js        nats.JetStreamContext
	kv        nats.KeyValue            // Default bucket (for backward compatibility)
	buckets   map[string]nats.KeyValue // Dynamic buckets map
	logger    logging.Logger
	mu        sync.RWMutex
	connected bool
}

// NewNATSClient creates a new NATS client with the given configuration.
func NewNATSClient(config *Config, cache *GroupCache, logger logging.Logger) (*NATSClient, error) {
	client := &NATSClient{
		config:  config,
		logger:  logger,
		buckets: make(map[string]nats.KeyValue),
	}

	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	logger.Info("Successfully connected to NATS")
	return client, nil
}

// connect establishes connection to NATS and sets up K/V store.
func (nc *NATSClient) connect() error {
	opts := []nats.Option{
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			nc.logger.Warn("NATS disconnected: %v", err)
			nc.setConnected(false)
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			nc.logger.Info("NATS reconnected")
			nc.setConnected(true)
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			nc.logger.Info("NATS connection closed")
			nc.setConnected(false)
		}),
		nats.MaxReconnects(nc.config.MaxReconnectAttempts),
		nats.ReconnectWait(time.Duration(nc.config.ReconnectWait)),
	}

	// Add authentication options
	if nc.config.Credentials != "" {
		opts = append(opts, nats.UserCredentials(nc.config.Credentials))
	} else if nc.config.Token != "" {
		opts = append(opts, nats.Token(nc.config.Token))
	} else if nc.config.Username != "" && nc.config.Password != "" {
		opts = append(opts, nats.UserInfo(nc.config.Username, nc.config.Password))
	}

	// Add TLS options
	if nc.config.TLSCert != "" && nc.config.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(nc.config.TLSCert, nc.config.TLSKey)
		if err != nil {
			return fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: nc.config.TLSInsecure,
		}

		if nc.config.TLSCACert != "" {
			opts = append(opts, nats.RootCAs(nc.config.TLSCACert))
		}

		opts = append(opts, nats.Secure(tlsConfig))
	}

	// Connect to NATS
	conn, err := nats.Connect(nc.config.ServerURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	nc.conn = conn
	nc.setConnected(true)

	// Create JetStream context
	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	nc.js = js

	// Get or create K/V bucket
	kv, err := js.KeyValue(nc.config.Bucket)
	if err != nil {
		// Try to create the bucket if it doesn't exist
		_, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: nc.config.Bucket,
			TTL:    time.Duration(nc.config.TTL),
		})
		if err != nil {
			return fmt.Errorf("failed to create K/V bucket: %w", err)
		}

		kv, err = js.KeyValue(nc.config.Bucket)
		if err != nil {
			return fmt.Errorf("failed to get K/V bucket: %w", err)
		}
	}
	nc.kv = kv

	if nc.config.EnableBucketRouting {
		nc.logger.Info("Connected to NATS at %s, default bucket: %s (bucket routing enabled - groups will use separate buckets)", nc.config.ServerURL, nc.config.Bucket)
	} else {
		nc.logger.Info("Connected to NATS at %s, using K/V bucket: %s", nc.config.ServerURL, nc.config.Bucket)
	}
	return nil
}

// Get retrieves a value from NATS K/V store.
func (nc *NATSClient) Get(path []string) (interface{}, error) {
	if !nc.isConnected() {
		return nil, fmt.Errorf("NATS client not connected")
	}

	// Determine which bucket to use
	bucketName := nc.determineBucket(path)
	kv, err := nc.getOrCreateBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}

	// Strip group prefix from path to get the actual key
	key := nc.pathToKeyInBucket(path, bucketName)

	entry, err := kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get key %s from bucket %s: %w", key, bucketName, err)
	}

	var value interface{}
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		// If JSON unmarshal fails, return as string
		return string(entry.Value()), nil
	}

	return value, nil
}

// Put stores a value in NATS K/V store.
func (nc *NATSClient) Put(path []string, value interface{}) error {
	if !nc.isConnected() {
		return fmt.Errorf("NATS client not connected")
	}

	// Determine which bucket to use
	bucketName := nc.determineBucket(path)
	kv, err := nc.getOrCreateBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}

	// Strip group prefix from path to get the actual key
	key := nc.pathToKeyInBucket(path, bucketName)

	var data []byte

	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		data, err = json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
	}

	_, err = kv.Put(key, data)
	if err != nil {
		return fmt.Errorf("failed to put key %s to bucket %s: %w", key, bucketName, err)
	}

	return nil
}

// Delete removes a value from NATS K/V store.
func (nc *NATSClient) Delete(path []string) error {
	if !nc.isConnected() {
		return fmt.Errorf("NATS client not connected")
	}

	// Determine which bucket to use
	bucketName := nc.determineBucket(path)
	kv, err := nc.getOrCreateBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}

	// Strip group prefix from path to get the actual key
	key := nc.pathToKeyInBucket(path, bucketName)

	err = kv.Delete(key)
	if err != nil && err != nats.ErrKeyNotFound {
		return fmt.Errorf("failed to delete key %s from bucket %s: %w", key, bucketName, err)
	}

	return nil
}

// pathToKey converts a path slice to a NATS K/V key.
func (nc *NATSClient) pathToKey(path []string) string {
	return strings.Join(path, ".")
}

// pathToKeyInBucket converts a path slice to a NATS K/V key, stripping the group prefix.
func (nc *NATSClient) pathToKeyInBucket(path []string, bucketName string) string {
	// If bucket routing is not enabled, use the full path
	if !nc.config.EnableBucketRouting {
		return nc.pathToKey(path)
	}

	// Try to extract group and find where it ends in the path
	if nc.config.GroupRegexPattern != "" {
		pathStr := strings.Join(path, ".")
		if re, err := regexp.Compile(nc.config.GroupRegexPattern); err == nil {
			matches := re.FindStringSubmatch(pathStr)
			if len(matches) > 1 {
				group := matches[1]
				// Find where the group appears in the path and take everything after it
				for i, segment := range path {
					if segment == group {
						// Take everything after the group segment
						if i+1 < len(path) {
							remainingPath := path[i+1:]
							key := strings.Join(remainingPath, ".")
							nc.logger.Debug("Stripped group '%s' from path %v, using key: %s", group, path, key)
							return key
						}
						// If nothing after group, use empty key
						return ""
					}
				}
			}
		}
	}

	// If single group mode or no pattern match, just use the full path
	// since the bucket separation is handled elsewhere
	if nc.config.SingleGroup != "" {
		// In single group mode, use the full path as key
		return nc.pathToKey(path)
	}

	// Fallback to full path
	return nc.pathToKey(path)
}

// keyToPath converts a NATS K/V key back to a path slice.
func (nc *NATSClient) keyToPath(key string) ([]string, error) {
	if key == "" {
		return []string{}, nil
	}
	return strings.Split(key, "."), nil
}

// isConnected returns the current connection status.
func (nc *NATSClient) isConnected() bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.connected
}

// setConnected sets the connection status.
func (nc *NATSClient) setConnected(connected bool) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.connected = connected
}

// getOrCreateBucket gets an existing bucket or creates a new one if it doesn't exist.
func (nc *NATSClient) getOrCreateBucket(bucketName string) (nats.KeyValue, error) {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	// Check if we already have this bucket cached
	if kv, exists := nc.buckets[bucketName]; exists {
		return kv, nil
	}

	// Try to get existing bucket
	kv, err := nc.js.KeyValue(bucketName)
	if err != nil {
		// Try to create the bucket if it doesn't exist
		_, err = nc.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: bucketName,
			TTL:    time.Duration(nc.config.TTL),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create K/V bucket %s: %w", bucketName, err)
		}

		kv, err = nc.js.KeyValue(bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get K/V bucket %s after creation: %w", bucketName, err)
		}
	}

	// Cache the bucket for future use
	nc.buckets[bucketName] = kv
	nc.logger.Debug("Using K/V bucket: %s", bucketName)

	return kv, nil
}

// determineBucket determines which bucket to use for a given path.
func (nc *NATSClient) determineBucket(path []string) string {
	// If bucket routing is not enabled, use the default bucket
	if !nc.config.EnableBucketRouting {
		nc.logger.Debug("Bucket routing disabled, using default bucket: %s", nc.config.Bucket)
		return nc.config.Bucket
	}

	// Extract group UUID from path using bucket regex patterns
	group := nc.extractGroupForBucket(path)
	if group == "" {
		// No group found, use default bucket
		nc.logger.Debug("No group found in path %v, using default bucket: %s", path, nc.config.Bucket)
		return nc.config.Bucket
	}

	// Construct bucket name with optional prefix
	var bucketName string
	if nc.config.BucketPrefix != "" {
		bucketName = nc.config.BucketPrefix + group
	} else {
		bucketName = group
	}

	nc.logger.Debug("Extracted group '%s' from path %v, routing to bucket: %s", group, path, bucketName)
	return bucketName
}

// extractGroupForBucket extracts the group from a path for bucket routing.
func (nc *NATSClient) extractGroupForBucket(path []string) string {
	// Use GroupRegexPattern for bucket routing (same pattern used for group watching)
	if nc.config.GroupRegexPattern == "" && nc.config.SingleGroup == "" {
		return ""
	}

	// If single group mode, always use that group
	if nc.config.SingleGroup != "" {
		return nc.config.SingleGroup
	}

	// Use group regex pattern to extract group
	pathStr := strings.Join(path, ".")
	if re, err := regexp.Compile(nc.config.GroupRegexPattern); err == nil {
		matches := re.FindStringSubmatch(pathStr)
		if len(matches) > 1 {
			// Return the first capture group which should be the group identifier
			return matches[1]
		}
	}

	return ""
}
