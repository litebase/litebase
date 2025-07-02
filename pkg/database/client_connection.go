package database

import (
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
)

type ClientConnection struct {
	accessKey  *auth.AccessKey
	BranchID   string
	connection *DatabaseConnection
	DatabaseID string
	path       string
}

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

func (d *ClientConnection) Checkpoint() error {
	return d.connection.Checkpoint()
}

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

func (d *ClientConnection) Path() string {
	return d.path
}

func (d *ClientConnection) Release() {
	d.connection.release()
}

func (d *ClientConnection) WithAccessKey(accessKey *auth.AccessKey) *ClientConnection {
	d.accessKey = accessKey
	d.connection.WithAccessKey(accessKey)

	return d
}
