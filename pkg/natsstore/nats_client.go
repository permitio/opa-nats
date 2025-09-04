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
type NATSClient struct {
	config    *Config
	conn      *nats.Conn
	js        nats.JetStreamContext
	buckets   map[string]nats.KeyValue // Dynamic buckets map
	logger    logging.Logger
	mu        sync.RWMutex
	connected bool
}

// NewNATSClient creates a new NATS client with the given configuration.
func NewNATSClient(config *Config, logger logging.Logger) (*NATSClient, error) {
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

func (nc *NATSClient) getBucket(bucketName string) (nats.KeyValue, error) {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	kv, err := nc.js.KeyValue(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get K/V bucket %s: %w", bucketName, err)
	}

	return kv, nil
}
