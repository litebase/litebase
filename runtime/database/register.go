package database

import (
	"litebasedb/runtime/sqlite3_vfs"
	"log"
)

var LitebaseDBVFS VFS

func Register(connection *Connection) {
	err := sqlite3_vfs.RegisterVFS("litebasedb", NewVFS(connection))

	if err != nil {
		log.Fatalf("Register VFS err: %s", err)
	}
}
