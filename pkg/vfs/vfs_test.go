package vfs_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/pkg/vfs"
)

func TestRegisterVFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		_, err := vfs.RegisterVFS("vfsId", "test", 4096, nil, nil)

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

		err = vfs.UnregisterVFS("vfsId")

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
		_, err := vfs.RegisterVFS("vfsId", "test", 4096, nil, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}

		_, err = vfs.RegisterVFS("vfsId", "test", 4096, nil, nil)

		if err != nil {
			t.Errorf("RegisterVFS() failed, expected nil, got %v", err)
		}
	})
}

func TestNewVfsErrors(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		_, err := vfs.RegisterVFS("vfsId", "test", 4096, nil, nil)

		if err != nil {
			t.Error(err)
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

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		test.RunQuery(db, []byte("CREATE TABLE users (id INT, name TEXT)"), []sqlite3.StatementParameter{})

		if len(offsets) == 0 {
			t.Errorf("SetWriteHook() failed, expected > 0, got %v", len(offsets))
		}
	})
}

func TestVFSFileSizeAndTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		storage.PageLoggerCompactInterval = 0

		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a set of tables to force the database to grow. SQLite will
		// create a new page for each table root page so this is good for our
		// test of the VFS file size and truncate.
		for i := 1; i <= 3000; i++ {
			// Create the table
			test.RunQuery(db, fmt.Appendf(nil, "CREATE TABLE users_%d (id INT, name TEXT)", i), []sqlite3.StatementParameter{})

			// Insert a row
			test.RunQuery(
				db,
				fmt.Appendf(nil, "INSERT INTO users_%d (id, name) VALUES (?, ?)", i),
				[]sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(i),
				}, {
					Type:  "TEXT",
					Value: []byte("user"),
				}},
			)
		}

		// Force the database to checkpoint so data is written to disk
		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		path := file.GetDatabaseFileDir(mock.DatabaseId, mock.BranchId)
		pageCount := db.GetConnection().FileSystem().Metadata().PageCount

		var expectedSize int64 = 4096 * pageCount
		var directorySize int64

		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()
		fileSystemDriver := dfs.FileSystem().Driver()

		fileSystemDriver.Flush()
		err = dfs.Compact()

		if err != nil {
			t.Fatalf("Compact failed, expected nil, got %v", err)
		}

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

			info, err := fileSystemDriver.Stat(path + entry.Name())

			if err != nil {
				t.Fatal(err)
			}

			directorySize += info.Size()
		}

		// Check if the directory size is greater than 0
		if directorySize == 0 {
			t.Fatalf("VFS file size failed, expected > 0, got %v", directorySize)
		}

		// Check if the directory size is equal to the expected size
		if directorySize != int64(expectedSize) {
			t.Errorf("VFS file size failed, expected %v, got %v", expectedSize, directorySize)
		}

		for i := 3000; i > 2000; i-- {
			// Drop the table
			test.RunQuery(db, fmt.Appendf(nil, "DROP TABLE users_%d", i), []sqlite3.StatementParameter{})
		}

		// Force the database to checkpoint so data is written to disk
		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		pageCount = db.GetConnection().FileSystem().Metadata().PageCount
		directorySize = 0
		expectedSize = 4096 * pageCount

		fileSystemDriver.Flush()
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

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a table for users
		test.RunQuery(db, []byte("CREATE TABLE users (id INT, name TEXT)"), []sqlite3.StatementParameter{})

		// Insert 10000 rows
		for i := range 10000 {
			_, err := db.GetConnection().Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				[]sqlite3.StatementParameter{
					{
						Type:  "INTEGER",
						Value: int64(i + 1),
					}, {
						Type:  "TEXT",
						Value: []byte("user"),
					},
				},
			)

			if err != nil {
				log.Fatalf("Insert %d failed, expected nil, got %v", i, err)
			}
			// result := test.RunQuery(db, []byte("INSERT INTO users (id, name) VALUES (?, ?)"), []sqlite3.StatementParameter{
			// 	{
			// 		Type:  "INTEGER",
			// 		Value: int64(i + 1),
			// 	}, {
			// 		Type:  "TEXT",
			// 		Value: []byte("user"),
			// 	},
			// })
		}

		result := test.RunQuery(db, []byte("SELECT * FROM users"), []sqlite3.StatementParameter{})

		if len(result.Rows) != 10000 {
			t.Errorf("Expected 10000 rows, got %v", len(result.Rows))
		}

		// Delete all rows
		test.RunQuery(db, []byte("DELETE FROM users"), []sqlite3.StatementParameter{})

		err = db.GetConnection().SqliteConnection().Vacuum()

		if err != nil {
			t.Errorf("VACUUM failed, expected nil, got %v", err)
		}

		// Check if the database is empty
		result = test.RunQuery(db, []byte("SELECT * FROM users"), []sqlite3.StatementParameter{})

		if len(result.Rows) != 0 {
			t.Errorf("VACUUM failed, expected 0, got %v", len(result.Rows))
		}

		db.Close()
	})
}
