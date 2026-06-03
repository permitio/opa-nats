package natsstore

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/v1/logging"
)

// NATSClient manages the connection to NATS and K/V operations.
//
// All tenants share a single muxed bucket (config.Bucket), keyed as
// "<tenant>.<key...>". The bucket handle is opened once and cached; per-tenant
// access is by filtering keys/watches on the "<tenant>." prefix.
type NATSClient struct {
	config    *Config
	conn      *nats.Conn
	js        nats.JetStreamContext
	bucket    nats.KeyValue // the single muxed bucket, lazily opened
	logger    logging.Logger
	mu        sync.RWMutex
	connected bool
}

// NewNATSClient creates a new NATS client with the given configuration.
func NewNATSClient(config *Config, logger logging.Logger) (*NATSClient, error) {
	client := &NATSClient{
		config: config,
		logger: logger,
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
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			nc.logger.Warn("NATS disconnected: %v", err)
			nc.setConnected(false)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			nc.logger.Info("NATS reconnected")
			nc.setConnected(true)
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			nc.logger.Info("NATS connection closed")
			nc.setConnected(false)
		}),
		nats.MaxReconnects(nc.config.MaxReconnectAttempts),
		nats.ReconnectWait(time.Duration(nc.config.ReconnectWait)),
	}

	// Add authentication options
	if nc.config.Credentials != "" {
		opts = append(opts, nats.UserCredentials(nc.config.Credentials))
	} else if nc.config.UserJwt != "" && nc.config.UserNkeySeed != "" {
		opts = append(opts, nats.UserJWTAndSeed(nc.config.UserJwt, nc.config.UserNkeySeed))
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

	nc.logger.Info("Connected to NATS at %s", nc.config.ServerURL)
	return nil
}

// keyToPath converts a NATS K/V key back to a path slice.
func (nc *NATSClient) keyToPath(key string) []string {
	if key == "" {
		return []string{}
	}
	return strings.Split(key, ".")
}

// setConnected sets the connection status.
func (nc *NATSClient) setConnected(connected bool) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.connected = connected
}

// getBucket opens (once) and returns the single muxed bucket handle.
func (nc *NATSClient) getBucket() (nats.KeyValue, error) {
	nc.mu.RLock()
	cached := nc.bucket
	nc.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.bucket != nil {
		return nc.bucket, nil
	}
	kv, err := nc.js.KeyValue(nc.config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to get K/V bucket %s: %w", nc.config.Bucket, err)
	}
	nc.bucket = kv
	return kv, nil
}

// tenantKeys lists the current keys belonging to one tenant ("<tenant>.*")
// WITHOUT enumerating the whole muxed bucket. It uses a single-filter watch
// drained to the initial-values marker, so the work is O(tenant), not O(all
// tenants). The returned keys are full muxed keys ("<tenant>.<rest>").
func (nc *NATSClient) tenantKeys(tenant string) ([]string, error) {
	kv, err := nc.getBucket()
	if err != nil {
		return nil, err
	}
	w, err := kv.Watch(tenant+".>", nats.IgnoreDeletes(), nats.MetaOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to list keys for tenant %s: %w", tenant, err)
	}
	defer func() { _ = w.Stop() }()

	var keys []string
	for entry := range w.Updates() {
		if entry == nil { // initial values delivered
			break
		}
		keys = append(keys, entry.Key())
	}
	return keys, nil
}
