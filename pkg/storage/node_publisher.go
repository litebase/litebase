package storage

// NodePublisher interface to avoid circular dependencies with cluster package
type NodePublisher interface {
	Publish(message any) (map[string]any, map[string]error)
	IsReplica() bool
	IsPrimary() bool
}

func WithNodePublisher(np NodePublisher) PageLogManagerConfig {
	return func(plm *PageLogManager) {
		plm.nodePublisher = np
	}
}
