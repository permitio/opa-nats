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
	ServerURL   string   `json:"server_url"`
	Credentials string   `json:"credentials,omitempty"`
	Token       string   `json:"token,omitempty"`
	Username    string   `json:"username,omitempty"`
	Password    string   `json:"password,omitempty"`
	TLSCert     string   `json:"tls_cert,omitempty"`
	TLSKey      string   `json:"tls_key,omitempty"`
	TLSCACert   string   `json:"tls_ca_cert,omitempty"`
	TLSInsecure bool     `json:"tls_insecure,omitempty"`

	// Cache settings
	TTL                  Duration `json:"ttl"`
	RefreshInterval      Duration `json:"refresh_interval"`
	MaxReconnectAttempts int      `json:"max_reconnect_attempts"`
	ReconnectWait        Duration `json:"reconnect_wait"`

	// Group-based watcher settings (MaxGroupWatchers is the LRU cache size for group watchers)
	MaxGroupWatchers      int    `json:"max_group_watchers,omitempty"`       // LRU cache size for group watchers
	GroupRegexPattern     string `json:"group_regex_pattern,omitempty"`      // Regex pattern to extract group from paths (also used for bucket routing)
	GroupWatcherCacheSize int    `json:"group_watcher_cache_size,omitempty"` // Cache size per group watcher

	// Single group mode (alternative to group patterns)
	SingleGroup string `json:"single_group,omitempty"` // Explicit group name to always use

	// Handled paths as regex (if empty, use default paths)
	HandledPathsRegex []string `json:"handled_paths_regex,omitempty"`
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		ServerURL:             "nats://localhost:4222",
		TTL:                   Duration(10 * time.Minute),
		RefreshInterval:       Duration(30 * time.Second),
		MaxReconnectAttempts:  10,
		ReconnectWait:         Duration(2 * time.Second),
		MaxGroupWatchers:      10,  // Maximum concurrent group watchers (cache size)
		GroupWatcherCacheSize: 100, // Cache size per group watcher
		GroupRegexPattern:     "",  // Must be configured by user
		SingleGroup:           "",  // Optional single group mode
		HandledPathsRegex:     nil, // default to nil, handled in logic
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.ServerURL == "" {
		return fmt.Errorf("server_url is required")
	}
	if c.GroupRegexPattern != "" && c.SingleGroup != "" {
		return fmt.Errorf("group_regex_pattern and single_group cannot be configured together")
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
