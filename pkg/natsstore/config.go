package natsstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration embeds time.Duration and makes it more JSON-friendly.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	val, err := time.ParseDuration(str)
	*d = Duration(val)
	return err
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

// Config represents the NATS K/V store plugin configuration.
type Config struct {
	// NATS connection settings
	ServerURL    string `json:"server_url"`
	Credentials  string `json:"credentials,omitempty"`
	Token        string `json:"token,omitempty"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	UserJwt      string `json:"user_jwt,omitempty"`
	UserNkeySeed string `json:"user_nkey_seed,omitempty"`
	TLSCert      string `json:"tls_cert,omitempty"`
	TLSKey       string `json:"tls_key,omitempty"`
	TLSCACert    string `json:"tls_ca_cert,omitempty"`
	TLSInsecure  bool   `json:"tls_insecure,omitempty"`

	// Domain is an optional JetStream domain. When set, JetStream API calls are
	// scoped to $JS.<domain>.API instead of the default $JS.API. Required when
	// the target cluster is reached across a leafnode boundary; harmless for
	// direct connections since the server serves its own domain prefix locally.
	// Empty preserves the default (domain-less) prefix.
	Domain string `json:"domain,omitempty"`

	// Cache settings
	TTL                  Duration `json:"ttl"`
	RefreshInterval      Duration `json:"refresh_interval"`
	MaxReconnectAttempts int      `json:"max_reconnect_attempts"`
	ReconnectWait        Duration `json:"reconnect_wait"`

	// Bucket is the single muxed NATS K/V bucket that holds every tenant's data,
	// keyed as "<tenant>.<key...>". Required. The first key token is the tenant,
	// which the Rego-facing layer addresses as the "bucket_id".
	Bucket string `json:"bucket"`

	// Tenant-watcher settings (MaxBucketsWatchers is the LRU cache size for per-tenant watchers)
	MaxBucketsWatchers int `json:"max_bucket_watchers,omitempty"` // LRU cache size for tenant watchers

	// RootTenant, if set, is a tenant whose subtree mounts at the OPA data root
	// (its leading tenant token is stripped) instead of under data.nats.kv.<tenant>.
	RootTenant string `json:"root_tenant,omitempty"`
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		ServerURL: "nats://localhost:4222",
		// Bucket is intentionally left empty: the muxed bucket name is MANDATORY
		// and must be set explicitly by the deployment config (validated as
		// "bucket is required"). There is no implicit default to avoid silently
		// reading the wrong bucket.
		TTL:                  Duration(10 * time.Minute),
		RefreshInterval:      Duration(30 * time.Second),
		MaxReconnectAttempts: 10,
		ReconnectWait:        Duration(2 * time.Second),
		MaxBucketsWatchers:   10, // Maximum concurrent per-tenant watchers (cache size)
		RootTenant:           "", // Optional: a tenant that mounts at the data root
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.ServerURL == "" {
		return fmt.Errorf("server_url is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	return nil
}

// ValidateWithDefaults validates the configuration after defaults have been applied.
func (c *Config) ValidateWithDefaults() error {
	if err := c.Validate(); err != nil {
		return err
	}
	return nil
}
