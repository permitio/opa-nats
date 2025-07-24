package natsstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/open-policy-agent/opa/v1/storage"
)

// MockStore is a naive in-memory store implementation for testing
type MockStore struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewMockStore creates a new mock store
func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string]interface{}),
	}
}

// NewTransaction creates a new transaction
func (m *MockStore) NewTransaction(ctx context.Context, params ...storage.TransactionParams) (storage.Transaction, error) {
	return &MockTransaction{store: m}, nil
}

// Read reads data from the store
func (m *MockStore) Read(ctx context.Context, txn storage.Transaction, path storage.Path) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := pathToKey(path)
	if value, exists := m.data[key]; exists {
		return value, nil
	}

	return nil, &storage.Error{
		Code:    storage.NotFoundErr,
		Message: fmt.Sprintf("path %v not found", path),
	}
}

// Write writes data to the store
func (m *MockStore) Write(ctx context.Context, txn storage.Transaction, op storage.PatchOp, path storage.Path, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := pathToKey(path)

	switch op {
	case storage.AddOp, storage.ReplaceOp:
		m.data[key] = value
	case storage.RemoveOp:
		delete(m.data, key)
	default:
		return fmt.Errorf("unsupported operation: %v", op)
	}

	return nil
}

// ListPolicies lists all policies (returns empty for mock)
func (m *MockStore) ListPolicies(ctx context.Context, txn storage.Transaction) ([]string, error) {
	return []string{}, nil
}

// GetPolicy gets a policy by ID (returns empty for mock)
func (m *MockStore) GetPolicy(ctx context.Context, txn storage.Transaction, id string) ([]byte, error) {
	return nil, &storage.Error{
		Code:    storage.NotFoundErr,
		Message: fmt.Sprintf("policy %s not found", id),
	}
}

// UpsertPolicy inserts or updates a policy (no-op for mock)
func (m *MockStore) UpsertPolicy(ctx context.Context, txn storage.Transaction, id string, policy []byte) error {
	return nil
}

// DeletePolicy deletes a policy (no-op for mock)
func (m *MockStore) DeletePolicy(ctx context.Context, txn storage.Transaction, id string) error {
	return nil
}

// Register registers a trigger (no-op for mock)
func (m *MockStore) Register(ctx context.Context, txn storage.Transaction, config storage.TriggerConfig) (storage.TriggerHandle, error) {
	return nil, nil
}

// Unregister unregisters a trigger (no-op for mock)
func (m *MockStore) Unregister(ctx context.Context, txn storage.Transaction, handle storage.TriggerHandle) {
}

// Commit commits a transaction (no-op for mock)
func (m *MockStore) Commit(ctx context.Context, txn storage.Transaction) error {
	return nil
}

// Abort aborts a transaction (no-op for mock)
func (m *MockStore) Abort(ctx context.Context, txn storage.Transaction) {
}

// Close closes the store (no-op for mock)
func (m *MockStore) Close(ctx context.Context, txn storage.Transaction) error {
	return nil
}

// Truncate truncates the store (no-op for mock)
func (m *MockStore) Truncate(ctx context.Context, txn storage.Transaction, params storage.TransactionParams, it storage.Iterator) error {
	return nil
}

// MockTransaction is a naive transaction implementation for testing
type MockTransaction struct {
	store *MockStore
}

// ID returns the transaction ID
func (t *MockTransaction) ID() uint64 {
	return 1
}

// Helper function to convert path to string key
func pathToKey(path storage.Path) string {
	if len(path) == 0 {
		return ""
	}

	result := ""
	for i, segment := range path {
		if i > 0 {
			result += "."
		}
		result += fmt.Sprintf("%v", segment)
	}
	return result
}

// GetData returns all data in the store (helper for testing)
func (m *MockStore) GetData() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]interface{})
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

// SetData sets data in the store (helper for testing)
func (m *MockStore) SetData(data map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make(map[string]interface{})
	for k, v := range data {
		m.data[k] = v
	}
}

// Clear clears all data in the store (helper for testing)
func (m *MockStore) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make(map[string]interface{})
}
