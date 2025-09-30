package storage

// MockNodePublisher is a mock implementation of NodePublisher for testing
type MockNodePublisher struct {
	isReplica bool
	isPrimary bool
	responses map[string]any
	errors    map[string]error
}

// NewMockNodePublisher creates a new mock node publisher
func NewMockNodePublisher() *MockNodePublisher {
	return &MockNodePublisher{
		isReplica: false,
		isPrimary: true,
		responses: make(map[string]any),
		errors:    make(map[string]error),
	}
}

// SetReplica sets whether this node is a replica
func (m *MockNodePublisher) SetReplica(isReplica bool) {
	m.isReplica = isReplica
	m.isPrimary = !isReplica
}

// SetResponse sets a response for a specific node ID
func (m *MockNodePublisher) SetResponse(nodeID string, response any) {
	m.responses[nodeID] = response
}

// SetError sets an error for a specific node ID
func (m *MockNodePublisher) SetError(nodeID string, err error) {
	m.errors[nodeID] = err
}

// Publish implements NodePublisher
func (m *MockNodePublisher) Publish(message any) (map[string]any, map[string]error) {
	return m.responses, m.errors
}

// IsReplica implements NodePublisher
func (m *MockNodePublisher) IsReplica() bool {
	return m.isReplica
}

// IsPrimary implements NodePublisher
func (m *MockNodePublisher) IsPrimary() bool {
	return m.isPrimary
}