package vector_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorIndexInsert(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create a vector index table with 3 dimensions for testing
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				embedding BLOB,
				dimensions=3,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert some test vectors (using VectorToBlob for proper encoding)
		testVectors := [][]float32{
			{1.0, 2.0, 3.0},
			{4.0, 5.0, 6.0},
			{7.0, 8.0, 9.0},
		}

		for i, vec := range testVectors {
			blob := VectorToBlob(vec)

			_, err = dbConn.Exec(
				"INSERT INTO product_vectors(embedding) VALUES(?)",
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeBlob, Value: blob},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i+1, err)
			}
		}

		// Verify vectors were inserted into _vectors table
		res, err := dbConn.Exec("SELECT COUNT(*) FROM product_vectors_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to count vectors: %v", err)
		}

		count := res.Rows[0][0].Int64()

		if count != 3 {
			t.Errorf("Expected 3 vectors in _vectors table, got %d", count)
		}

		// Verify all vectors are assigned to a valid cluster (cluster_id > 0).
		// Vectors are now assigned inline to real leaf clusters via goAssignVectorsInBatch,
		// so cluster_id=0 is never written.
		res, err = dbConn.Exec("SELECT COUNT(*) FROM product_vectors_embedding_cluster_vector_map WHERE cluster_id > 0", nil)

		if err != nil {
			t.Fatalf("Failed to count cluster assignments: %v", err)
		}

		clusterCount := res.Rows[0][0].Int64()

		if clusterCount != 3 {
			t.Errorf("Expected 3 vectors assigned to real clusters, got %d", clusterCount)
		}

		// Verify we can read the vectors back
		res, err = dbConn.Exec("SELECT id, embedding FROM product_vectors_vectors ORDER BY id", nil)

		if err != nil {
			t.Fatalf("Failed to query vectors: %v", err)
		}

		if len(res.Rows) != 3 {
			t.Errorf("Expected to find 3 vectors, found %d", len(res.Rows))
		}

		for i, row := range res.Rows {
			id := row[0].Int64()
			vectorBlob := row[1].Blob()

			if len(vectorBlob) == 0 {
				t.Errorf("Vector %d has empty blob", i+1)
			}

			t.Logf("Found vector id=%d, blob length=%d bytes", id, len(vectorBlob))
		}
	})
}

func TestVectorIndexDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create index
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				embedding BLOB,
				dimensions=3,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert vectors
		vec1 := VectorToBlob([]float32{1.0, 2.0, 3.0})
		vec2 := VectorToBlob([]float32{4.0, 5.0, 6.0})

		_, err = dbConn.Exec("INSERT INTO product_vectors(embedding) VALUES(?)",
			[]sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}},
		)

		if err != nil {
			t.Fatalf("Failed to insert vector 1: %v", err)
		}

		_, err = dbConn.Exec("INSERT INTO product_vectors(embedding) VALUES(?)",
			[]sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec2}},
		)

		if err != nil {
			t.Fatalf("Failed to insert vector 2: %v", err)
		}

		// Verify we have 2 vectors
		res, err := dbConn.Exec("SELECT COUNT(*) FROM product_vectors_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to count vectors before delete: %v", err)
		}

		countBefore := res.Rows[0][0].Int64()

		if countBefore != 2 {
			t.Errorf("Expected 2 vectors before delete, got %d", countBefore)
		}

		// Delete a vector by rowid
		_, err = dbConn.Exec("DELETE FROM product_vectors WHERE rowid = 1", nil)

		if err != nil {
			t.Fatalf("Failed to delete vector: %v", err)
		}

		// Verify we now have 1 vector
		res, err = dbConn.Exec("SELECT COUNT(*) FROM product_vectors_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to count vectors after delete: %v", err)
		}

		countAfter := res.Rows[0][0].Int64()

		if countAfter != 1 {
			t.Errorf("Expected 1 vector after delete, got %d", countAfter)
		}

		// Verify the deleted vector is removed from cluster mapping
		res, err = dbConn.Exec("SELECT COUNT(*) FROM product_vectors_embedding_cluster_vector_map", nil)

		if err != nil {
			t.Fatalf("Failed to count cluster mappings: %v", err)
		}

		mappingCount := res.Rows[0][0].Int64()

		if mappingCount != 1 {
			t.Errorf("Expected 1 cluster mapping after delete, got %d", mappingCount)
		}

		// Verify the remaining vector is id=2
		res, err = dbConn.Exec("SELECT id FROM product_vectors_vectors ORDER BY id", nil)

		if err != nil {
			t.Fatalf("Failed to query remaining vectors: %v", err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}

		remainingID := res.Rows[0][0].Int64()

		if remainingID != 2 {
			t.Errorf("Expected remaining vector to have id=2, got %d", remainingID)
		}
	})
}

func TestVectorIndexUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create index
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE product_vectors USING vector_index(
				embedding BLOB,
				dimensions=3,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert a vector
		vec1 := VectorToBlob([]float32{1.0, 2.0, 3.0})

		_, err = dbConn.Exec("INSERT INTO product_vectors(embedding) VALUES(?)",
			[]sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}},
		)

		if err != nil {
			t.Fatalf("Failed to insert vector: %v", err)
		}

		// Verify we have 1 vector
		res, err := dbConn.Exec("SELECT COUNT(*) FROM product_vectors_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to count vectors before update: %v", err)
		}

		countBefore := res.Rows[0][0].Int64()

		if countBefore != 1 {
			t.Errorf("Expected 1 vector before update, got %d", countBefore)
		}

		// Get the original vector ID
		res, err = dbConn.Exec("SELECT id FROM product_vectors_vectors LIMIT 1", nil)

		if err != nil {
			t.Fatalf("Failed to get original vector ID: %v", err)
		}

		originalID := res.Rows[0][0].Int64()

		// Update the vector with new values
		vec2 := VectorToBlob([]float32{4.0, 5.0, 6.0})

		_, err = dbConn.Exec("UPDATE product_vectors SET embedding = ? WHERE rowid = ?",
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeBlob, Value: vec2},
				{Type: sqlite3.ParameterTypeInteger, Value: originalID},
			},
		)

		if err != nil {
			t.Fatalf("Failed to update vector: %v", err)
		}

		// The _vectors row is updated in place, so the count stays at 1.
		res, err = dbConn.Exec("SELECT COUNT(*) FROM product_vectors_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to count vectors after update: %v", err)
		}

		countAfter := res.Rows[0][0].Int64()

		if countAfter != 1 {
			t.Errorf("Expected 1 vector after update, got %d", countAfter)
		}

		// There should be exactly 1 cluster mapping, assigned to a real cluster.
		res, err = dbConn.Exec("SELECT COUNT(*) FROM product_vectors_embedding_cluster_vector_map WHERE cluster_id > 0", nil)

		if err != nil {
			t.Fatalf("Failed to count cluster mappings: %v", err)
		}

		mappingCount := res.Rows[0][0].Int64()

		if mappingCount != 1 {
			t.Errorf("Expected 1 cluster mapping after update with cluster_id > 0, got %d", mappingCount)
		}

		// The rowid must be unchanged after an in-place update.
		res, err = dbConn.Exec("SELECT id, embedding FROM product_vectors_vectors LIMIT 1", nil)

		if err != nil {
			t.Fatalf("Failed to query updated vector: %v", err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}

		updatedID := res.Rows[0][0].Int64()
		updatedBlob := res.Rows[0][1].Blob()

		if updatedID != originalID {
			t.Errorf("Expected rowid to be unchanged after update: original=%d, got=%d", originalID, updatedID)
		}

		if len(updatedBlob) == 0 {
			t.Error("Updated vector has empty blob")
		}

		t.Logf("Updated vector blob length: %d bytes", len(updatedBlob))
	})
}
