package natsstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/open-policy-agent/opa/v1/logging"
)

// MockNATSClient is a mock implementation of NATS client for testing
type MockNATSClient struct {
	mu        sync.RWMutex
	data      map[string]interface{}
	started   bool
	startErr  error
	getErr    error
	putErr    error
	deleteErr error
	logger    logging.Logger
	onUpdate  func(key string, value interface{})
}

// NewMockNATSClient creates a new mock NATS client
func NewMockNATSClient(logger logging.Logger) *MockNATSClient {
	return &MockNATSClient{
		data:   make(map[string]interface{}),
		logger: logger,
	}
}

// Start starts the mock NATS client
func (m *MockNATSClient) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.startErr != nil {
		return m.startErr
	}

	m.started = true
	return nil
}

// Stop stops the mock NATS client
func (m *MockNATSClient) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.started = false
	return nil
}

// Get retrieves a value from the mock store
func (m *MockNATSClient) Get(path []string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.getErr != nil {
		return nil, m.getErr
	}

	key := pathToStringKey(path)
	if value, exists := m.data[key]; exists {
		return value, nil
	}

	return nil, errors.New("key not found")
}

// Put stores a value in the mock store
func (m *MockNATSClient) Put(path []string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putErr != nil {
		return m.putErr
	}

	key := pathToStringKey(path)
	m.data[key] = value

	// Trigger update callback if set
	if m.onUpdate != nil {
		go m.onUpdate(key, value)
	}

	return nil
}

// Delete removes a value from the mock store
func (m *MockNATSClient) Delete(path []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteErr != nil {
		return m.deleteErr
	}

	key := pathToStringKey(path)
	delete(m.data, key)

	return nil
}

// IsStarted returns whether the client is started
func (m *MockNATSClient) IsStarted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}

// Helper methods for testing
func (m *MockNATSClient) SetData(data map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]interface{})
	for k, v := range data {
		m.data[k] = v
	}
}

func (m *MockNATSClient) GetData() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]interface{})
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *MockNATSClient) SetErrors(startErr, getErr, putErr, deleteErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startErr = startErr
	m.getErr = getErr
	m.putErr = putErr
	m.deleteErr = deleteErr
}

func (m *MockNATSClient) SetUpdateCallback(callback func(key string, value interface{})) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onUpdate = callback
}

// Helper function to convert path to string key
func pathToStringKey(path []string) string {
	if len(path) == 0 {
		return ""
	}

	result := ""
	for i, segment := range path {
		if i > 0 {
			result += "."
		}
		result += segment
	}
	return result
}

// Test the actual NATSClient interface compliance
func TestNATSClientInterface(t *testing.T) {
	logger := logging.Get()

	// Test that MockNATSClient implements the expected interface
	var _ interface {
		Start(context.Context) error
		Stop(context.Context) error
		Get([]string) (interface{}, error)
		Put([]string, interface{}) error
		Delete([]string) error
		IsStarted() bool
	} = NewMockNATSClient(logger)
}

func TestMockNATSClient_StartStop(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Test initial state
	if client.IsStarted() {
		t.Error("Client should not be started initially")
	}

	// Test start
	err := client.Start(ctx)
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}

	if !client.IsStarted() {
		t.Error("Client should be started after Start()")
	}

	// Test stop
	err = client.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}

	if client.IsStarted() {
		t.Error("Client should not be started after Stop()")
	}
}

func TestMockNATSClient_StartError(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Set start error
	expectedErr := errors.New("start error")
	client.SetErrors(expectedErr, nil, nil, nil)

	// Test start with error
	err := client.Start(ctx)
	if err != expectedErr {
		t.Errorf("Expected start error %v, got %v", expectedErr, err)
	}

	if client.IsStarted() {
		t.Error("Client should not be started after error")
	}
}

func TestMockNATSClient_GetPut(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Start client
	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Test put
	path := []string{"data", "schemas", "user"}
	value := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id":   map[string]interface{}{"type": "string"},
			"name": map[string]interface{}{"type": "string"},
		},
	}

	err = client.Put(path, value)
	if err != nil {
		t.Errorf("Put should not return error: %v", err)
	}

	// Test get
	retrieved, err := client.Get(path)
	if err != nil {
		t.Errorf("Get should not return error: %v", err)
	}

	if retrieved == nil {
		t.Error("Retrieved value should not be nil")
	}

	// Test get non-existent key
	_, err = client.Get([]string{"non", "existent", "key"})
	if err == nil {
		t.Error("Get should return error for non-existent key")
	}
}

func TestMockNATSClient_Delete(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Start client
	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Put test data
	path := []string{"data", "test", "key"}
	value := "test value"

	err = client.Put(path, value)
	if err != nil {
		t.Errorf("Put should not return error: %v", err)
	}

	// Verify data exists
	_, err = client.Get(path)
	if err != nil {
		t.Errorf("Get should not return error before delete: %v", err)
	}

	// Delete data
	err = client.Delete(path)
	if err != nil {
		t.Errorf("Delete should not return error: %v", err)
	}

	// Verify data is deleted
	_, err = client.Get(path)
	if err == nil {
		t.Error("Get should return error after delete")
	}
}

func TestMockNATSClient_Errors(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Start client
	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Test get error
	getErr := errors.New("get error")
	client.SetErrors(nil, getErr, nil, nil)

	_, err = client.Get([]string{"test"})
	if err != getErr {
		t.Errorf("Expected get error %v, got %v", getErr, err)
	}

	// Test put error
	putErr := errors.New("put error")
	client.SetErrors(nil, nil, putErr, nil)

	err = client.Put([]string{"test"}, "value")
	if err != putErr {
		t.Errorf("Expected put error %v, got %v", putErr, err)
	}

	// Test delete error
	deleteErr := errors.New("delete error")
	client.SetErrors(nil, nil, nil, deleteErr)

	err = client.Delete([]string{"test"})
	if err != deleteErr {
		t.Errorf("Expected delete error %v, got %v", deleteErr, err)
	}
}

func TestMockNATSClient_UpdateCallback(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Start client
	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Set update callback
	var mu sync.Mutex
	callbackCalled := false
	var callbackKey string
	var callbackValue interface{}

	client.SetUpdateCallback(func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		callbackCalled = true
		callbackKey = key
		callbackValue = value
	})

	// Put data to trigger callback
	path := []string{"data", "test", "callback"}
	value := "callback test"

	err = client.Put(path, value)
	if err != nil {
		t.Errorf("Put should not return error: %v", err)
	}

	// Wait for callback to be called
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	called := callbackCalled
	key := callbackKey
	val := callbackValue
	mu.Unlock()

	if !called {
		t.Error("Update callback should have been called")
	}

	expectedKey := pathToStringKey(path)
	if key != expectedKey {
		t.Errorf("Expected callback key %s, got %s", expectedKey, key)
	}

	if val != value {
		t.Errorf("Expected callback value %v, got %v", value, val)
	}
}

func TestMockNATSClient_ConcurrentAccess(t *testing.T) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	// Start client
	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Test concurrent reads and writes
	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup

	// Start writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{"data", "concurrent", fmt.Sprintf("key%d_%d", id, j)}
				value := fmt.Sprintf("value%d_%d", id, j)

				err := client.Put(path, value)
				if err != nil {
					t.Errorf("Put error in goroutine %d: %v", id, err)
				}
			}
		}(i)
	}

	// Start readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := []string{"data", "concurrent", fmt.Sprintf("key%d_%d", id, j)}

				// Try to read, may not exist yet
				_, err := client.Get(path)
				// Error is expected if key doesn't exist yet
				_ = err
			}
		}(i)
	}

	wg.Wait()

	// Verify some data was written
	data := client.GetData()
	if len(data) == 0 {
		t.Error("Expected some data to be written")
	}
}

// Benchmark tests
func BenchmarkMockNATSClient_Put(b *testing.B) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	err := client.Start(ctx)
	if err != nil {
		b.Fatalf("Failed to start client: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := []string{"benchmark", "put", fmt.Sprintf("key%d", i)}
		value := fmt.Sprintf("value%d", i)

		err := client.Put(path, value)
		if err != nil {
			b.Errorf("Put error: %v", err)
		}
	}
}

func BenchmarkMockNATSClient_Get(b *testing.B) {
	logger := logging.Get()
	client := NewMockNATSClient(logger)
	ctx := context.Background()

	err := client.Start(ctx)
	if err != nil {
		b.Fatalf("Failed to start client: %v", err)
	}

	// Pre-populate data
	for i := 0; i < 1000; i++ {
		path := []string{"benchmark", "get", fmt.Sprintf("key%d", i)}
		value := fmt.Sprintf("value%d", i)

		err := client.Put(path, value)
		if err != nil {
			b.Fatalf("Put error during setup: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := []string{"benchmark", "get", fmt.Sprintf("key%d", i%1000)}

		_, err := client.Get(path)
		if err != nil {
			b.Errorf("Get error: %v", err)
		}
	}
}

// TestBucketRouting tests the bucket routing functionality
func TestBucketRouting(t *testing.T) {
	// Create a mock logger that implements the logging.Logger interface
	mockLogger := &MockLogger{}

	tests := []struct {
		name           string
		config         *Config
		path           []string
		expectedBucket string
	}{
		{
			name: "bucket routing disabled - use default bucket",
			config: &Config{
				GroupRegexPattern:   "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:           []string{"groups", "550e8400-e29b-41d4-a716-446655440000", "metadata"},
			expectedBucket: "default-bucket",
		},
		{
			name: "bucket routing enabled - extract group ID",
			config: &Config{
				GroupRegexPattern:   "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:           []string{"groups", "550e8400-e29b-41d4-a716-446655440000", "metadata"},
			expectedBucket: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "bucket routing enabled with prefix",
			config: &Config{
				GroupRegexPattern:   "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:           []string{"groups", "550e8400-e29b-41d4-a716-446655440000", "members", "user1"},
			expectedBucket: "group-550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "no group match - use default bucket",
			config: &Config{
				GroupRegexPattern:   "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:           []string{"users", "123e4567-e89b-12d3-a456-426614174000", "groups"},
			expectedBucket: "default-bucket",
		},
		{
			name: "single group mode",
			config: &Config{
				SingleGroup:         "my-single-group",
			},
			path:           []string{"groups", "anything", "metadata"},
			expectedBucket: "my-single-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &NATSClient{
				config: tt.config,
				logger: mockLogger,
			}

			bucket := client.determineBucket(tt.path)
			if bucket != tt.expectedBucket {
				t.Errorf("determineBucket() = %s, want %s", bucket, tt.expectedBucket)
			}
		})
	}
}

// TestExtractGroupForBucket tests the group extraction logic for bucket routing
func TestExtractGroupForBucket(t *testing.T) {
	mockLogger := &MockLogger{}

	tests := []struct {
		name          string
		config        *Config
		path          []string
		expectedGroup string
	}{
		{
			name: "extract ID from groups path",
			config: &Config{
				GroupRegexPattern: "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:          []string{"groups", "550e8400-e29b-41d4-a716-446655440000", "metadata"},
			expectedGroup: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "no regex pattern configured",
			config: &Config{
				GroupRegexPattern: "",
			},
			path:          []string{"groups", "550e8400-e29b-41d4-a716-446655440000", "metadata"},
			expectedGroup: "",
		},
		{
			name: "path doesn't match pattern",
			config: &Config{
				GroupRegexPattern: "^groups\\.([0-9a-f-]+)\\..*",
			},
			path:          []string{"users", "550e8400-e29b-41d4-a716-446655440000", "groups"},
			expectedGroup: "",
		},
		{
			name: "single group mode",
			config: &Config{
				SingleGroup: "my-group",
			},
			path:          []string{"groups", "anything", "metadata"},
			expectedGroup: "my-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &NATSClient{
				config: tt.config,
				logger: mockLogger,
			}

			group := client.extractGroupForBucket(tt.path)
			if group != tt.expectedGroup {
				t.Errorf("extractGroupForBucket() = %s, want %s", group, tt.expectedGroup)
			}
		})
	}
}

// MockLogger implements the logging.Logger interface for testing
type MockLogger struct{}

func (m *MockLogger) Debug(msg string, args ...any)                   {}
func (m *MockLogger) Info(msg string, args ...any)                    {}
func (m *MockLogger) Warn(msg string, args ...any)                    {}
func (m *MockLogger) Error(msg string, args ...any)                   {}
func (m *MockLogger) WithFields(fields map[string]any) logging.Logger { return m }
func (m *MockLogger) GetLevel() logging.Level                         { return logging.Debug }
func (m *MockLogger) SetLevel(level logging.Level)                    {}
func (m *MockLogger) GetFields() map[string]any                       { return nil }
