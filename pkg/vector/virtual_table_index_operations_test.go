package vector_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestVectorIndexInsert(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		db, err := sql.Open("litebase-internal", testDb.DatabaseID+"/"+testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				t.Fatalf("Failed to close database: %v", err)
			}
		}()

		// Create a vector index table with 3 dimensions for testing
		_, err = db.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				vector BLOB,
				dimensions=3,
				distance_metric='l2'
			)
		`)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert some test vectors
		testVectors := []struct {
			vector string
		}{
			{"[1.0, 2.0, 3.0]"},
			{"[4.0, 5.0, 6.0]"},
			{"[7.0, 8.0, 9.0]"},
		}

		for i, tv := range testVectors {
			_, err = db.Exec(
				"INSERT INTO product_vectors(vector) VALUES(?)",
				tv.vector,
			)

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i+1, err)
			}
		}

		// Verify vectors were inserted into pending table
		var count int

		err = db.QueryRow("SELECT COUNT(*) FROM product_vectors_pending").Scan(&count)

		if err != nil {
			t.Fatalf("Failed to count pending vectors: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected 3 vectors in pending table, got %d", count)
		}

		// Verify metadata has correct pending_count
		var pendingCount string

		err = db.QueryRow("SELECT value FROM product_vectors_metadata WHERE key='pending_count'").Scan(&pendingCount)

		if err != nil {
			t.Fatalf("Failed to read pending_count: %v", err)
		}

		if pendingCount != "3" {
			t.Errorf("Expected pending_count=3, got %s", pendingCount)
		}

		// Verify we can read the vectors back
		rows, err := db.Query("SELECT id, vector_blob FROM product_vectors_pending ORDER BY id")

		if err != nil {
			t.Fatalf("Failed to query pending vectors: %v", err)
		}

		defer func() {
			if err := rows.Close(); err != nil {
				t.Fatalf("Failed to close rows: %v", err)
			}
		}()

		foundVectors := 0

		for rows.Next() {
			var id int64
			var vectorBlob []byte

			err = rows.Scan(&id, &vectorBlob)

			if err != nil {
				t.Errorf("Failed to scan row: %v", err)
				continue
			}

			foundVectors++
			fmt.Printf("Found vector id=%d, vector_blob length=%d bytes\n", id, len(vectorBlob))
		}

		if foundVectors != 3 {
			t.Errorf("Expected to find 3 vectors, found %d", foundVectors)
		}
	})
}

func TestVectorIndexUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				t.Fatalf("Failed to close database: %v", err)
			}
		}()

		// Create index
		_, err = db.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				vector BLOB,
				dimensions=3,
				distance_metric='l2'
			)
		`)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert a vector and capture its auto-generated rowid
		var rowid int64
		err = db.QueryRow(
			"INSERT INTO product_vectors(vector) VALUES(?) RETURNING rowid",
			"[1.0, 2.0, 3.0]",
		).Scan(&rowid)

		if err != nil {
			t.Fatalf("Failed to insert vector: %v", err)
		}

		// Update the vector using the rowid
		_, err = db.Exec(
			"UPDATE product_vectors SET vector = ? WHERE rowid = ?",
			"[4.0, 5.0, 6.0]",
			rowid,
		)

		if err != nil {
			t.Fatalf("Failed to update vector: %v", err)
		}

		// Verify the vector was updated in pending table
		var vectorBlob []byte

		err = db.QueryRow("SELECT vector_blob FROM product_vectors_pending WHERE id = 1").Scan(&vectorBlob)

		if err != nil {
			t.Fatalf("Failed to read updated vector: %v", err)
		}

		if len(vectorBlob) == 0 {
			t.Errorf("Expected non-empty vector_blob after update")
		}
	})
}

func TestVectorIndexDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				t.Fatalf("Failed to close database: %v", err)
			}
		}()

		// Create index
		_, err = db.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				vector BLOB,
				dimensions=3,
				distance_metric='l2'
			)
		`)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert vectors
		_, err = db.Exec("INSERT INTO product_vectors(vector) VALUES('[1.0, 2.0, 3.0]')")

		if err != nil {
			t.Fatalf("Failed to insert vector 1: %v", err)
		}

		_, err = db.Exec("INSERT INTO product_vectors(vector) VALUES( '[4.0, 5.0, 6.0]')")

		if err != nil {
			t.Fatalf("Failed to insert vector 2: %v", err)
		}

		// Delete a vector
		_, err = db.Exec("DELETE FROM product_vectors WHERE rowid = 1")

		if err != nil {
			t.Fatalf("Failed to delete vector: %v", err)
		}

		// Verify we have 2 entries: one DELETE for id=1, one INSERT for id=2
		var count int

		err = db.QueryRow("SELECT COUNT(*) FROM product_vectors_pending").Scan(&count)

		if err != nil {
			t.Fatalf("Failed to count pending entries: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected 2 pending entries (1 DELETE + 1 INSERT), got %d", count)
		}

		// Verify id=1 has operation='DELETE'
		var operation string

		err = db.QueryRow("SELECT operation FROM product_vectors_pending WHERE id = 1").Scan(&operation)

		if err != nil {
			t.Fatalf("Failed to read operation for id=1: %v", err)
		}

		if operation != "DELETE" {
			t.Errorf("Expected operation='DELETE' for id=1, got %s", operation)
		}

		// Verify id=2 has operation='INSERT'
		err = db.QueryRow("SELECT operation FROM product_vectors_pending WHERE id = 2").Scan(&operation)

		if err != nil {
			t.Fatalf("Failed to read operation for id=2: %v", err)
		}

		if operation != "INSERT" {
			t.Errorf("Expected operation='INSERT' for id=2, got %s", operation)
		}
	})
}
