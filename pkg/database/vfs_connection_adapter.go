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
func (vca *VfsConnectionAdapter) Get(databaseID, branchID string) (interface{}, error) {
	conn, err := vca.connectionManager.Get(databaseID, branchID)

	if err != nil {
		return nil, err
	}

	// Return the ClientConnection itself, not the underlying DatabaseConnection
	return conn, nil
}

// Release releases a database connection back to the pool
func (vca *VfsConnectionAdapter) Release(conn interface{}) {
	clientConn, ok := conn.(*ClientConnection)

	if !ok {
		slog.Error("Invalid connection type in Release", "type", conn)
		return
	}

	// Release the client connection directly
	vca.connectionManager.Release(clientConn)
}
