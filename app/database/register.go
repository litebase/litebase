package database

import (
	"log"

	"github.com/psanford/sqlite3vfs"
)

var LitebaseDBVFS VFS

func Register() {
	vfs := New()

	err := sqlite3vfs.RegisterVFS("litebasedb", vfs)

	if err != nil {
		log.Fatalf("Register VFS err: %s", err)
	}
}
