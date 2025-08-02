package vfs_test

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
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

		filesystem := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()

		filesystem.SetWriteHook(func(offset int64, data []byte) {
			offsets = append(offsets, offset)
		})

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		_, err = db.GetConnection().Exec("CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		if err != nil {
			t.Fatal(err)
		}

		if len(offsets) == 0 {
			t.Errorf("SetWriteHook() failed, expected > 0, got %v", len(offsets))
		}
	})
}

func TestVFSFileSizeAndTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		// Create a set of tables to force the database to grow. SQLite will
		// create a new page for each table root page so this is good for our
		// test of the VFS file size and truncate.
		for i := 1; i <= 3000; i++ {
			// Create the table
			_, err := db.GetConnection().Exec(
				fmt.Sprintf("CREATE TABLE users_%d (id INT, name TEXT)", i),
				[]sqlite3.StatementParameter{},
			)

			if err != nil {
				t.Fatal(err)
			}

			// Insert a row
			_, err = db.GetConnection().Exec(
				fmt.Sprintf("INSERT INTO users_%d (id, name) VALUES (?, ?)", i),
				[]sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(i),
				}, {
					Type:  "TEXT",
					Value: []byte("user"),
				}},
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		// Force the database to checkpoint so data is written to disk
		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		path := file.GetDatabaseFileDir(mock.DatabaseID, mock.DatabaseBranchID)
		pageCount := db.GetConnection().FileSystem().Metadata().PageCount

		var expectedSize int64 = 4096 * pageCount
		var directorySize int64

		dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
		fileSystemDriver := dfs.FileSystem().Driver()

		fileSystemDriver.Flush()
		err = dfs.ForceCompact()

		if err != nil {
			t.Fatalf("ForceCompact failed, expected nil, got %v", err)
		}

		// Get the file size of the directory using the range index
		rangeIndex := dfs.RangeManager.Index

		entries, err := rangeIndex.All()

		if err != nil {
			t.Fatal(err)
		}

		if len(entries) == 0 {
			t.Fatalf("Range index is empty, expected > 0, got %v", len(entries))
		}

		for _, entry := range entries {
			rangePath := fmt.Sprintf("%s%s", path, entry.Name())

			info, err := fileSystemDriver.Stat(rangePath)

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
			_, err := db.GetConnection().Exec(
				fmt.Sprintf("DROP TABLE users_%d", i),
				[]sqlite3.StatementParameter{},
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		// Force the database to checkpoint so data is written to disk
		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		pageCount = db.GetConnection().FileSystem().Metadata().PageCount
		directorySize = 0
		expectedSize = 4096 * pageCount

		fileSystemDriver.Flush()

		rangeIndex = dfs.RangeManager.Index

		entries, err = rangeIndex.All()

		if err != nil {
			t.Fatal(err)
		}

		if len(entries) == 0 {
			t.Fatalf("Range index is empty, expected > 0, got %v", len(entries))
		}

		for _, entry := range entries {
			rangePath := fmt.Sprintf("%s%s", path, entry.Name())

			info, err := fileSystemDriver.Stat(rangePath)

			if err != nil {
				t.Fatal(err)
			}

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

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		// Create a table for users
		_, err = db.GetConnection().Exec(
			"CREATE TABLE users (id INT, name TEXT)",
			[]sqlite3.StatementParameter{},
		)

		if err != nil {
			t.Fatal(err)
		}

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
		}

		result, err := db.GetConnection().Exec("SELECT * FROM users", []sqlite3.StatementParameter{})

		if err != nil {
			t.Fatal(err)
		}

		if len(result.Rows) != 10000 {
			t.Errorf("Expected 10000 rows, got %v", len(result.Rows))
		}

		// Delete all rows
		_, err = db.GetConnection().Exec("DELETE FROM users", []sqlite3.StatementParameter{})

		if err != nil {
			t.Fatal(err)
		}

		err = db.GetConnection().Vacuum()

		if err != nil {
			t.Errorf("VACUUM failed, expected nil, got %v", err)
		}

		// Check if the database is empty
		result, err = db.GetConnection().Exec("SELECT * FROM users", []sqlite3.StatementParameter{})

		if err != nil {
			t.Fatal(err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("VACUUM failed, expected 0, got %v", len(result.Rows))
		}

		db.Close()
	})
}

func TestVFSLocking(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1)

		con2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con2)

		con1.GetConnection().BusyTimeout(0 * time.Second)
		con2.GetConnection().BusyTimeout(0 * time.Second)

		t.Run("LockintTransactions", func(t *testing.T) {
			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				err := con1.GetConnection().BeginImmediate()

				if err != nil {
					t.Errorf("Begin transaction failed in goroutine 1, expected nil, got %v", err)
				}

				time.Sleep(100 * time.Millisecond)
			}()

			go func() {
				defer wg.Done()

				time.Sleep(50 * time.Millisecond)
				err := con2.GetConnection().Begin()

				if err != nil {
					log.Printf("Expected error in goroutine 2: %v", err)
				}

				_, err = con2.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

				if err == nil {
					t.Errorf("Begin transaction should have failed in goroutine 2, expected error, got nil")
				}
			}()

			wg.Wait()

			err = con1.GetConnection().Commit()

			if err != nil {
				t.Errorf("Commit transaction failed in goroutine 1, expected nil, got %v", err)
			}
		})

		t.Run("LockingCheckpoint", func(t *testing.T) {
			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				err := con1.GetConnection().BeginImmediate()

				if err != nil {
					t.Errorf("Begin transaction failed in goroutine 1, expected nil, got %v", err)
				}

				time.Sleep(100 * time.Millisecond)
			}()

			go func() {
				defer wg.Done()

				time.Sleep(50 * time.Millisecond)
				err := con2.GetConnection().Checkpoint()

				if err == nil {
					t.Errorf("Checkpoint should have failed in goroutine 2, expected error, got nil")
				}
			}()

			wg.Wait()

			err = con1.GetConnection().Commit()

			if err != nil {
				t.Errorf("Commit transaction failed in goroutine 1, expected nil, got %v", err)
			}
		})
	})
}
