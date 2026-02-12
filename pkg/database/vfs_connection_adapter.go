package database

import (
	"log/slog"
)

// VfsConnectionAdapter adapts database.ConnectionManager to vfs.ConnectionManager interface
type VfsConnectionAdapter struct {
	connectionManager *ConnectionManager
}

// NewVfsConnectionAdapter creates a new adapter for VFS connection management
func NewVfsConnectionAdapter(cm *ConnectionManager) *VfsConnectionAdapter {
	return &VfsConnectionAdapter{
		connectionManager: cm,
	}
}

// Get retrieves a database connection for the given database and branch IDs
func (vca *VfsConnectionAdapter) Get(databaseID, branchID string) (any, error) {
	clientConn, err := vca.connectionManager.Get(databaseID, branchID)

	if err != nil {
		return nil, err
	}

	// Return the ClientConnection directly now that vector code lives
	// in the same package. This avoids unnecessary wrapping and keeps
	// the connection pool release mechanics straightforward.
	return clientConn, nil
}

// Release releases a database connection back to the pool
func (vca *VfsConnectionAdapter) Release(conn any) {
	// Accept either a ClientConnection directly or, for safety, handle
	// the legacy VectorConnectionAdapter type if present.
	if clientConn, ok := conn.(*ClientConnection); ok {
		vca.connectionManager.Release(clientConn)
		return
	}

	slog.Error("Invalid connection type in Release", "type", conn)
}
