package natsstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuration_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		duration Duration
		expected string
	}{
		{
			name:     "5 minutes",
			duration: Duration(5 * time.Minute),
			expected: `"5m0s"`,
		},
		{
			name:     "30 seconds",
			duration: Duration(30 * time.Second),
			expected: `"30s"`,
		},
		{
			name:     "zero duration",
			duration: Duration(0 * time.Second),
			expected: `"0s"`,
		},
		{
			name:     "1 hour",
			duration: Duration(time.Hour),
			expected: `"1h0m0s"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.duration)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		jsonData    string
		expected    Duration
		expectError bool
	}{
		{
			name:     "5 minutes",
			jsonData: `"5m"`,
			expected: Duration(5 * time.Minute),
		},
		{
			name:     "30 seconds",
			jsonData: `"30s"`,
			expected: Duration(30 * time.Second),
		},
		{
			name:     "1 hour",
			jsonData: `"1h"`,
			expected: Duration(time.Hour),
		},
		{
			name:        "invalid duration string",
			jsonData:    `"invalid"`,
			expectError: true,
		},
		{
			name:        "invalid json",
			jsonData:    `invalid`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := json.Unmarshal([]byte(tt.jsonData), &d)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, d)
			}
		})
	}
}

func TestDuration_String(t *testing.T) {
	tests := []struct {
		name     string
		duration Duration
		expected string
	}{
		{
			name:     "5 minutes",
			duration: Duration(5 * time.Minute),
			expected: "5m0s",
		},
		{
			name:     "30 seconds",
			duration: Duration(30 * time.Second),
			expected: "30s",
		},
		{
			name:     "zero duration",
			duration: Duration(0 * time.Second),
			expected: "0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.duration.String())
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: &Config{
				ServerURL: "nats://localhost:4222",
			},
			expectError: false,
		},
		{
			name: "missing server_url",
			config: &Config{
				TTL: Duration(5 * time.Minute),
			},
			expectError: true,
			errorMsg:    "server_url is required",
		},
		{
			name: "empty server_url",
			config: &Config{
				ServerURL: "",
			},
			expectError: true,
			errorMsg:    "server_url is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_ValidateWithDefaults(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: &Config{
				ServerURL: "nats://localhost:4222",
			},
			expectError: false,
		},
		{
			name: "missing server_url",
			config: &Config{
				TTL: Duration(5 * time.Minute),
			},
			expectError: true,
			errorMsg:    "server_url is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.ValidateWithDefaults()

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_JSONMarshaling(t *testing.T) {
	config := &Config{
		ServerURL:            "nats://test:4222",
		TTL:                  Duration(5 * time.Minute),
		RefreshInterval:      Duration(10 * time.Second),
		MaxReconnectAttempts: 5,
		ReconnectWait:        Duration(1 * time.Second),
		MaxBucketsWatchers:   20,
		RootBucket:           "test-bucket",
	}

	// Marshal to JSON
	data, err := json.Marshal(config)
	require.NoError(t, err)

	// Unmarshal back
	var unmarshaled Config
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)

	assert.Equal(t, config.ServerURL, unmarshaled.ServerURL)
	assert.Equal(t, config.TTL, unmarshaled.TTL)
	assert.Equal(t, config.RefreshInterval, unmarshaled.RefreshInterval)
	assert.Equal(t, config.MaxReconnectAttempts, unmarshaled.MaxReconnectAttempts)
	assert.Equal(t, config.ReconnectWait, unmarshaled.ReconnectWait)
	assert.Equal(t, config.MaxBucketsWatchers, unmarshaled.MaxBucketsWatchers)
	assert.Equal(t, config.RootBucket, unmarshaled.RootBucket)
}
