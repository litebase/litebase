package database

import (
	"litebasedb/server/auth"
)

type ClientConnection struct {
	accessKey    auth.AccessKey
	branchUuid   string
	connection   *DatabaseConnection
	databaseUuid string
	path         string
}

func NewClientConnection(
	databaseUuid string,
	branchUuid string,
) (*ClientConnection, error) {
	connection, err := NewDatabaseConnection(databaseUuid, branchUuid)

	if connection == nil {
		return nil, err
	}

	return &ClientConnection{
		branchUuid:   branchUuid,
		connection:   connection,
		databaseUuid: databaseUuid,
	}, nil
}

func (d *ClientConnection) Close() {
	if d.connection == nil {
		return
	}

	d.connection.Close()
}

func (d *ClientConnection) GetBranchUuid() string {
	return d.branchUuid
}

func (d *ClientConnection) GetConnection() *DatabaseConnection {
	return d.connection
}

func (d *ClientConnection) GetDatabaseUuid() string {
	return d.databaseUuid
}

func (d *ClientConnection) Path() string {
	return d.path
}

func (d *ClientConnection) WithAccessKey(accessKey auth.AccessKey) *ClientConnection {
	d.accessKey = accessKey

	d.connection.WithAccessKey(accessKey)
	d.connection.setAuthorizer()

	return d
}
