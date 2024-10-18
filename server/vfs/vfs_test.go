package vfs_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/file"
	"litebase/server/sqlite3"
	_ "litebase/server/sqlite3"
	"litebase/server/vfs"
	"testing"
)

func TestRegisterVFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dataPath := app.Config.DataPath

		err := vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil, nil)

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
	test.RunWithApp(t, func(app *server.App) {
		dataPath := app.Config.DataPath

		err := vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}

		err = vfs.RegisterVFS("connectionId", "vfsId", dataPath, 4096, nil, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}
	})
}

func TestNewVfsErrors(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := vfs.RegisterVFS("", "test", "test", 4096, nil, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "", "test", 4096, nil, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "test", "", 4096, nil, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}

		err = vfs.RegisterVFS("test", "test", "test", 0, nil, nil)

		if err == nil {
			t.Errorf("RegisterVFS() failed, expected error, got nil")
		}
	})
}

func TestGoWriteHook(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		offsets := make([]int64, 0)

		filesystem := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()

		filesystem.SetWriteHook(func(offset int64, data []byte) {
			offsets = append(offsets, offset)
		})

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		if len(offsets) == 0 {
			t.Errorf("SetWriteHook() failed, expected > 0, got %v", len(offsets))
		}
	})
}

func TestVFSFileSizeAndTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		// Create a set of tables to force the database to grow. SQLite will
		// create a new page for each table root page so this is good for our
		// test of the VFS file size and truncate.
		for i := 1; i <= 3000; i++ {
			// Create the table
			test.RunQuery(db, fmt.Sprintf("CREATE TABLE users_%d (id INT, name TEXT)", i), []sqlite3.StatementParameter{})

			// Insert a row
			test.RunQuery(db, fmt.Sprintf("INSERT INTO users_%d (id, name) VALUES (?, ?)", i), []sqlite3.StatementParameter{{
				Type:  "INTEGER",
				Value: i,
			}, {
				Type:  "TEXT",
				Value: "user",
			}})
		}

		// Force the database to checkpoint so data is written to disk
		err = db.Checkpoint()

		if err != nil {
			t.Fatal(err)
		}

		path := file.GetDatabaseFileDir(mock.DatabaseId, mock.BranchId)

		var expectedPages int64 = 3068 // Discovered through manual testing
		var expectedSize int64 = 4096 * expectedPages
		var directorySize int64

		fileSystemDriver := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().Driver()

		entries, err := fileSystemDriver.ReadDir(path)

		if err != nil {
			t.Fatal(err)
		}

		// Get the file size of the directory
		for _, entry := range entries {
			// Skip directories or files that start with an underscore
			if entry.IsDir() || entry.Name()[0] == '_' {
				continue
			}

			info, _ := fileSystemDriver.Stat(path + "/" + entry.Name())

			directorySize += info.Size()
		}

		// Check if the directory size is greater than 0
		if directorySize == 0 {
			t.Errorf("VFS file size failed, expected > 0, got %v", directorySize)
		}

		// Check if the directory size is equal to the expected size
		if directorySize != int64(expectedSize) {
			t.Errorf("VFS file size failed, expected %v, got %v", expectedSize, directorySize)
		}

		for i := 3000; i > 2000; i-- {
			// Drop the table
			test.RunQuery(db, fmt.Sprintf("DROP TABLE users_%d", i), []sqlite3.StatementParameter{})
		}

		// Force the database to checkpoint so data is written to disk
		err = db.Checkpoint()

		if err != nil {
			t.Fatal(err)
		}

		// Vacuum the database
		err = db.GetConnection().SqliteConnection().Vacuum()

		if err != nil {
			t.Errorf("VACUUM failed, expected nil, got %v", err)
		}

		err = db.Checkpoint()

		if err != nil {
			t.Fatal(err)
		}

		directorySize = 0
		expectedPages = 1023 // Discovered through manual testing
		expectedSize = 4096 * expectedPages

		entries, err = fileSystemDriver.ReadDir(path)

		if err != nil {
			t.Fatal(err)
		}

		// Get the file size of the directory
		for _, entry := range entries {
			// Skip directories or files that start with an underscore
			if entry.IsDir() || entry.Name()[0] == '_' {
				continue
			}

			info, _ := fileSystemDriver.Stat(path + "/" + entry.Name())

			directorySize += info.Size()
		}

		// Check if the directory size is greater than 0
		if directorySize == 0 {
			t.Errorf("VFS file size failed, expected > 0, got %v", directorySize)
		}

		// Check if the directory size is equal to the expected size
		if directorySize != int64(expectedSize) {
			t.Errorf("VFS file size failed, expected %v, got %v", expectedSize, directorySize)
		}

		db.Close()
	})
}

func TestVfsVacuum(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		// Create a table for users
		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		// Insert 10000 rows
		for i := 0; i < 10000; i++ {
			test.RunQuery(db, "INSERT INTO users (id, name) VALUES (?, ?)", []sqlite3.StatementParameter{{
				Type:  "INTEGER",
				Value: i}, {
				Type:  "TEXT",
				Value: "user",
			}})
		}

		result := test.RunQuery(db, "SELECT * FROM users", []sqlite3.StatementParameter{})

		if len(result.Rows) != 10000 {
			t.Errorf("VACUUM failed, expected 0, got %v", len(result.Rows))
		}

		// Delete all rows
		test.RunQuery(db, "DELETE FROM users", []sqlite3.StatementParameter{})

		err = db.GetConnection().SqliteConnection().Vacuum()

		if err != nil {
			t.Errorf("VACUUM failed, expected nil, got %v", err)
		}

		// Check if the database is empty
		result = test.RunQuery(db, "SELECT * FROM users", []sqlite3.StatementParameter{})

		if len(result.Rows) != 0 {
			t.Errorf("VACUUM failed, expected 0, got %v", len(result.Rows))
		}

		db.Close()
	})
}
