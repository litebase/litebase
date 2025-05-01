package vfs_test

import (
	"strconv"
	"testing"

	"github.com/litebase/litebase/server/vfs"

	"github.com/litebase/litebase/server/sqlite3"

	"github.com/litebase/litebase/internal/test"
)

func TestVFSShareMemoryCanBeCopiedFromOneInstanceAndAppliedToAnother(t *testing.T) {
	test.Run(t, func() {
		testPrimaryServer := test.NewTestServer(t)
		testReplicaServer := test.NewTestServer(t)
		mockDB := test.MockDatabase(testPrimaryServer.App)

		app1 := testPrimaryServer.App
		app2 := testReplicaServer.App

		db1, err := app1.DatabaseManager.ConnectionManager().Get(mockDB.DatabaseId, mockDB.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		// Create a test table
		_, err = db1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("Failed to create table on primary server: %v", err)
		}

		db2, err := app2.DatabaseManager.ConnectionManager().Get(mockDB.DatabaseId, mockDB.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			app1.DatabaseManager.ConnectionManager().Release(mockDB.DatabaseId, mockDB.BranchId, db1)
			app2.DatabaseManager.ConnectionManager().Release(mockDB.DatabaseId, mockDB.BranchId, db2)
		}()

		_, err = db1.GetConnection().Exec("SELECT * FROM test", nil)

		if err != nil {
			t.Fatalf("Failed to query primary server: %v", err)
		}

		// See if the table exists on the replica server
		_, err = db2.GetConnection().Exec("SELECT * FROM test", nil)

		if err != nil {
			t.Fatalf("Failed to query replica server: %v", err)
		}

		// Check if the row exists on the replica server
		result, err := db2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Fatalf("Failed to query replica server for row count: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatalf("No rows found on replica server")
		}

		if result.Rows[0][0].Int64() != 0 {
			t.Fatalf("Expected 0 rows on replica server, got %d", result.Rows[0][0].Int64())
		}

		// Insert a row into the primary server
		_, err = db1.GetConnection().Exec("INSERT INTO test (name) VALUES ('Test User')", nil)

		if err != nil {
			t.Fatalf("Failed to insert row into primary server: %v", err)
		}

		vfs1 := vfs.VfsMap[db1.GetConnection().VFSHash()]
		vfs2 := vfs.VfsMap[db2.GetConnection().VFSHash()]
		regions1 := vfs1.GetWALShmRegions()
		vfs2.SetWALShmRegions(regions1)

		// Check if the row exists on the replica server
		result, err = db2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Fatalf("Failed to query replica server for row count: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatalf("No rows found on replica server")
		}

		if result.Rows[0][0].Int64() != 1 {
			t.Fatalf("Expected 1 row on replica server, got %d", result.Rows[0][0].Int64())
		}

		// Insert 1000 more rows into the primary server
		for i := range 1000 {
			_, err = db1.GetConnection().
				Exec("INSERT INTO test (name) VALUES (?)", []sqlite3.StatementParameter{{
					Type:  "TEXT",
					Value: []byte("Test User " + strconv.Itoa(i)),
				}})

			if err != nil {
				t.Fatalf("Failed to insert row %d into primary server: %v", i, err)
			}

			regions1 = vfs1.GetWALShmRegions()
			vfs2.SetWALShmRegions(regions1)

			// Check if the row exists on the replica server
			result, err = db2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatalf("Failed to query replica server for row count: %v", err)
			}

			if len(result.Rows) == 0 {
				t.Fatalf("No rows found on replica server")
			}

			if result.Rows[0][0].Int64() != int64(i+2) {
				t.Fatalf("Expected %d rows on replica server, got %d", i+2, result.Rows[0][0].Int64())
			}
		}

		result, err = db2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Fatalf("Failed to query replica server for final row count: %v", err)
		}

		if result.Rows[0][0].Int64() != 1001 {
			t.Fatalf("Expected 1002 rows on replica server, got %d", result.Rows[0][0].Int64())
		}
	})
}
