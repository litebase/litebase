package database

import (
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
)

type ClientConnection struct {
	BranchID   string
	connection *DatabaseConnection
	DatabaseID string
}

// Create a new instance of a ClientConnection.
func NewClientConnection(
	connectionManager *ConnectionManager,
	databaseId string,
	branchId string,
) (*ClientConnection, error) {
	connection, err := NewDatabaseConnection(
		connectionManager,
		databaseId,
		branchId,
	)

	if connection == nil {
		return nil, err
	}

	return &ClientConnection{
		BranchID:   branchId,
		connection: connection,
		DatabaseID: databaseId,
	}, nil
}

// Checkpoint the client connection.
func (d *ClientConnection) Checkpoint() error {
	return d.connection.Checkpoint()
}

// Close the client connection.
func (d *ClientConnection) Close() {
	if d == nil || d.connection == nil {
		return
	}

	if d.connection.Closed() {
		return
	}

	err := d.connection.Close()

	if err != nil {
		slog.Error("Error closing database connection", "error", err)
	}
}

// Return the underlying DatabaseConnection instance.
func (d *ClientConnection) GetConnection() *DatabaseConnection {
	return d.connection
}

// Set an access key for the client connection.
func (d *ClientConnection) WithCredential(credential *auth.Credential) *ClientConnection {
	d.connection.WithCredential(credential)

	return d
}
