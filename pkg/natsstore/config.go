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
	ServerURL   string `json:"server_url"`
	Credentials string `json:"credentials,omitempty"`
	Token       string `json:"token,omitempty"`
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	TLSCert     string `json:"tls_cert,omitempty"`
	TLSKey      string `json:"tls_key,omitempty"`
	TLSCACert   string `json:"tls_ca_cert,omitempty"`
	TLSInsecure bool   `json:"tls_insecure,omitempty"`

	// Cache settings
	TTL                  Duration `json:"ttl"`
	RefreshInterval      Duration `json:"refresh_interval"`
	MaxReconnectAttempts int      `json:"max_reconnect_attempts"`
	ReconnectWait        Duration `json:"reconnect_wait"`

	// Bucket-based watcher settings (MaxBucketsWatchers is the LRU cache size for bucket watchers)
	MaxBucketsWatchers int `json:"max_bucket_watchers,omitempty"` // LRU cache size for bucket watchers

	// root bucket name
	RootBucket string `json:"root_bucket,omitempty"` // Explicit bucket name to always use

}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		ServerURL:            "nats://localhost:4222",
		TTL:                  Duration(10 * time.Minute),
		RefreshInterval:      Duration(30 * time.Second),
		MaxReconnectAttempts: 10,
		ReconnectWait:        Duration(2 * time.Second),
		MaxBucketsWatchers:   10, // Maximum concurrent bucket watchers (cache size)
		RootBucket:           "", // Optional single bucket mode
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.ServerURL == "" {
		return fmt.Errorf("server_url is required")
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
