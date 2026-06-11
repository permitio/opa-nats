package natsstore

import (
	"context"
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
			// Drop the cached bucket handle so the next getBucket re-opens it;
			// the JetStream context/handle may have gone stale across the drop.
			nc.mu.Lock()
			nc.bucket = nil
			nc.mu.Unlock()
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

	// Create JetStream context, scoped to the configured domain when set
	var jsOpts []nats.JSOpt
	if nc.config.Domain != "" {
		jsOpts = append(jsOpts, nats.Domain(nc.config.Domain))
	}
	js, err := conn.JetStream(jsOpts...)
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

// validateTenant rejects tenant ids that are unsafe as a single NATS subject
// token. Real tenants are environment UUID hex (only [0-9a-f]); this fail-closed
// guard stops a dotted/wildcard/whitespace id from making the subject-token
// watch filter ("<tenant>.>") and the plain string-prefix read paths disagree,
// which could otherwise read another tenant's subtree or match every tenant.
func validateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("empty tenant id")
	}
	if strings.ContainsAny(tenant, ".*> \t\r\n") {
		return fmt.Errorf(
			"invalid tenant id %q: must be a single NATS subject token "+
				"(no '.', '*', '>', or whitespace)", tenant)
	}
	return nil
}

// tenantKeys lists the current keys belonging to one tenant ("<tenant>.*")
// WITHOUT enumerating the whole muxed bucket. It uses a single-filter watch
// drained to the initial-values marker, so the work is O(tenant), not O(all
// tenants). The returned keys are full muxed keys ("<tenant>.<rest>").
//
// It binds the watch to ctx and selects on ctx.Done() so a stall cannot block a
// Rego query / plugin Start indefinitely. A channel close BEFORE the nil
// initial-values marker (e.g. a connection drop) is treated as an error rather
// than silently returning a truncated key set — partial tenant data in an authz
// plugin can flip allow/deny.
func (nc *NATSClient) tenantKeys(ctx context.Context, tenant string) ([]string, error) {
	if err := validateTenant(tenant); err != nil {
		return nil, err
	}
	kv, err := nc.getBucket()
	if err != nil {
		return nil, err
	}
	w, err := kv.Watch(tenant+".>", nats.IgnoreDeletes(), nats.MetaOnly(), nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to list keys for tenant %s: %w", tenant, err)
	}
	defer func() { _ = w.Stop() }()

	var keys []string
	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok {
				return nil, fmt.Errorf(
					"watch for tenant %s closed before initial values completed", tenant)
			}
			if entry == nil { // initial values delivered
				return keys, nil
			}
			keys = append(keys, entry.Key())
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
