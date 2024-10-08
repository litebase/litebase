package database

import (
	"litebase/server/auth"
)

type ClientConnection struct {
	accessKey  *auth.AccessKey
	BranchId   string
	connection *DatabaseConnection
	DatabaseId string
	path       string
}

func NewClientConnection(
	databaseId string,
	branchId string,
) (*ClientConnection, error) {
	connection, err := NewDatabaseConnection(databaseId, branchId)

	if connection == nil {
		return nil, err
	}

	return &ClientConnection{
		BranchId:   branchId,
		connection: connection,
		DatabaseId: databaseId,
	}, nil
}

func (d *ClientConnection) Checkpoint() error {
	return d.connection.Checkpoint()
}

func (d *ClientConnection) Close() {
	if d == nil || d.connection == nil {
		return
	}

	d.connection.Close()
}

func (d *ClientConnection) GetConnection() *DatabaseConnection {
	return d.connection
}

func (d *ClientConnection) Path() string {
	return d.path
}

func (d *ClientConnection) WithAccessKey(accessKey *auth.AccessKey) *ClientConnection {
	d.accessKey = accessKey

	d.connection.WithAccessKey(accessKey)
	// TODO: This needs to be implemented
	// d.connection.setAuthorizer()

	return d
}
