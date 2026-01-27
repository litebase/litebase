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

	return conn.connection, nil
}

// Release releases a database connection back to the pool
func (vca *VfsConnectionAdapter) Release(conn interface{}) {
	dbConn, ok := conn.(*DatabaseConnection)

	if !ok {
		slog.Error("Invalid connection type in Release", "type", conn)
		return
	}

	// Find the client connection that wraps this database connection
	// by searching through all database groups and branches
	vca.connectionManager.mutex.RLock()
	defer vca.connectionManager.mutex.RUnlock()

	for _, dbGroup := range vca.connectionManager.databases {
		dbGroup.mutex.Lock()

		for _, branchConnections := range dbGroup.branches {
			for _, clientConn := range branchConnections {
				if clientConn.connection.connection == dbConn {
					vca.connectionManager.Release(clientConn.connection)
					dbGroup.mutex.Unlock()

					return
				}
			}
		}

		dbGroup.mutex.Unlock()
	}

	slog.Warn("Connection not found in pool during Release")
}
