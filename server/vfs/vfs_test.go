package vfs_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/database"
	_ "litebase/server/sqlite3"
	"litebase/server/vfs"
	"testing"
)

func TestRegisterVFS(t *testing.T) {
	test.Run(t, func() {
		dataPath := config.Get().DataPath

		err := vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}

		if len(vfs.VfsMap) != 1 {
			t.Errorf("RegisterVFS() failed, expected 1, got %v", len(vfs.VfsMap))
		}

		if vfs.VfsMap["vfsId"] == nil {
			t.Errorf("RegisterVFS() failed, expected not nil, got nil")
		}

		// Check SQLite to see if the VFS was registered
		if !vfs.VFSIsRegistered("vfsId") {
			t.Errorf("RegisterVFS() failed, expected not nil, got nil")
		}

		err = vfs.UnregisterVFS("connectionId", "vfsId")

		if err != nil {
			t.Errorf("UnregisterVFS() failed, expected nil, got %v", err)
		}

		if len(vfs.VfsMap) != 0 {
			t.Errorf("UnregisterVFS() failed, expected 0, got %v", len(vfs.VfsMap))
		}

		if vfs.VFSIsRegistered("vfsId") {
			t.Errorf("UnregisterVFS() failed, expected nil, got %v", vfs.VFSIsRegistered("vfsId"))
		}
	})
}

func TestRegisterVFSTwiceReturnsNoError(t *testing.T) {
	test.Run(t, func() {
		dataPath := config.Get().DataPath

		err := vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}

		err = vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}
	})
}

func TestNewVfsErrors(t *testing.T) {
	test.Run(t, func() {
		err := vfs.RegisterVFS("", "test", "test", 4096, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "", "test", 4096, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "test", "", 4096, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "test", "test", 0, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}
	})
}

func TestGoWriteHook(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatal(err)
		}

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

	})
}
