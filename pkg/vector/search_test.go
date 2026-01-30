package vector_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorSearchBruteForceFallback(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`CREATE VIRTUAL TABLE embeddings USING vector_index(vector BLOB, dimensions=4, distance_metric=0)`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert a few test vectors
		testVectors := [][]float32{
			{1.0, 0.0, 0.0, 0.0},
			{0.0, 1.0, 0.0, 0.0},
			{0.0, 0.0, 1.0, 0.0},
			{0.0, 0.0, 0.0, 1.0},
			{0.5, 0.5, 0.0, 0.0},
		}

		for i, vec := range testVectors {
			jsonVec := VectorToJSON(vec)

			_, err := dbConn.Exec("INSERT INTO embeddings(vector) VALUES(vector_f32(?))", []sqlite3.StatementParameter{{Type: "TEXT", Value: []byte(jsonVec)}})

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		// Check what's in the pending table FIRST
		pendingRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_pending", nil)

		if err != nil {
			t.Fatalf("Failed to count pending vectors: %v", err)
		}

		if len(pendingRes.Rows) <= 0 {
			t.Fatal("No rows in pending count result")
		}

		// CRITICAL: Verify NO clusters exist yet (testing brute-force fallback)
		clustersRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_clusters", nil)

		if err != nil {
			t.Fatalf("Failed to count clusters: %v", err)
		}

		if len(clustersRes.Rows) > 0 && len(clustersRes.Rows[0]) > 0 {
			clusterCount := clustersRes.Rows[0][0].Int64()

			if clusterCount != 0 {
				t.Fatalf("Expected 0 clusters (testing brute-force fallback), got %d", clusterCount)
			}
		}

		// Verify count
		res, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings", nil)

		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No count result")
		}

		if len(res.Rows[0]) == 0 {
			t.Fatal("Count result has no columns")
		}

		countCol := res.Rows[0][0]

		if countCol == nil {
			t.Fatal("Count column is nil")
		}

		count := countCol.Int64()

		if count != 5 {
			t.Fatalf("Expected 5 vectors, got %d", count)
		}

		// Test vector_search() - should use brute-force fallback since no clusters exist
		queryVec := []float32{1.0, 0.0, 0.0, 0.0}
		queryJSON := VectorToJSON(queryVec)
		res, err = dbConn.Exec(
			"SELECT rowid, distance FROM vector_search('embeddings', 'vector', vector_f32(?), 3)",
			[]sqlite3.StatementParameter{{Type: "TEXT", Value: []byte(queryJSON)}},
		)

		if err != nil {
			t.Fatalf("vector_search failed: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("vector_search returned no results - brute-force fallback not working")
		}

		// Verify we got results
		if len(res.Rows) != 3 {
			t.Fatalf("Expected 3 results (k=3), got %d - brute-force fallback not limiting correctly", len(res.Rows))
		}

		// First result should be the exact match (rowid=1, distance=0)
		closestRowID := res.Rows[0][0].Int64()
		closestDistance := res.Rows[0][1].Float64()

		if closestRowID != 1 {
			t.Fatalf("Expected closest vector to have rowid 1, got %d", closestRowID)
		}

		if closestDistance != 0.0 {
			t.Fatalf("Expected exact match with distance 0, got %f", closestDistance)
		}
	})
}

func TestVectorSearchWithClusters(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`CREATE VIRTUAL TABLE embeddings USING vector_index(vector BLOB, dimensions=4, distance_metric=0)`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert test vectors
		testVectors := [][]float32{
			{1.0, 0.0, 0.0, 0.0},
			{0.0, 1.0, 0.0, 0.0},
			{0.0, 0.0, 1.0, 0.0},
			{0.0, 0.0, 0.0, 1.0},
			{0.5, 0.5, 0.0, 0.0},
		}

		for i, vec := range testVectors {
			jsonVec := VectorToJSON(vec)

			_, err := dbConn.Exec("INSERT INTO embeddings(vector) VALUES(vector_f32(?))", []sqlite3.StatementParameter{{Type: "TEXT", Value: []byte(jsonVec)}})

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		// Mark the index as having pending vectors to trigger processing
		app.VectorIndexMgr.MarkPending(mock.DatabaseID, mock.DatabaseBranchID, "embeddings")

		// Wait for the VectorIndexer job to complete (up to 5 seconds)
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	waitForIndexing:
		for {
			select {
			case <-timeout:
				t.Fatal("Timed out waiting for vector indexing to complete")
			case <-ticker.C:
				// Check if vectors have been indexed
				indexedRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_indexed", nil)

				if err != nil {
					t.Fatalf("Failed to count indexed vectors: %v", err)
				}

				if len(indexedRes.Rows) > 0 && len(indexedRes.Rows[0]) > 0 {
					indexedCount := indexedRes.Rows[0][0].Int64()

					if indexedCount > 0 {
						break waitForIndexing
					}
				}
			}
		}

		// Check what's in the pending table after indexing
		pendingRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_pending", nil)

		if err != nil {
			t.Fatalf("Failed to count pending vectors: %v", err)
		}

		if len(pendingRes.Rows) > 0 && len(pendingRes.Rows[0]) > 0 && pendingRes.Rows[0][0].Int64() != 0 {
			t.Fatalf("Expected 0 pending vectors after indexing, but found %d", pendingRes.Rows[0][0].Int64())
		}

		// Verify clusters were created
		clustersRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_clusters", nil)

		if err != nil {
			t.Fatalf("Failed to count clusters: %v", err)
		}

		if len(clustersRes.Rows) == 0 || len(clustersRes.Rows[0]) == 0 {
			t.Fatal("No cluster count result")
		}

		clusterCount := clustersRes.Rows[0][0].Int64()

		if clusterCount == 0 {
			t.Fatal("Expected at least 1 cluster after indexing")
		}

		// Verify indexed vectors
		indexedRes2, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_indexed", nil)

		if err != nil {
			t.Fatalf("Failed to count indexed vectors: %v", err)
		}

		if len(indexedRes2.Rows) == 0 || len(indexedRes2.Rows[0]) == 0 {
			t.Fatal("No indexed count result")
		}

		indexedCount2 := indexedRes2.Rows[0][0].Int64()

		if indexedCount2 == 0 {
			t.Fatal("Expected at least 1 indexed vector")
		}

		// Now test vector_search() - should use cluster-based search, not fallback
		queryVec := []float32{1.0, 0.0, 0.0, 0.0}
		queryJSON := VectorToJSON(queryVec)
		res, err := dbConn.Exec(
			"SELECT rowid, distance FROM vector_search('embeddings', 'vector', vector_f32(?), 3)",
			[]sqlite3.StatementParameter{{Type: "TEXT", Value: []byte(queryJSON)}},
		)

		if err != nil {
			t.Fatalf("vector_search failed: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("vector_search returned no results from cluster-based search")
		}

		// Verify we got results
		if len(res.Rows) != 3 {
			t.Fatalf("Expected 3 results (k=3), got %d - cluster-based search not limiting correctly", len(res.Rows))
		}

		// First result should be the exact match (rowid=1, distance=0)
		closestRowID := res.Rows[0][0].Int64()
		closestDistance := res.Rows[0][1].Float64()

		if closestRowID != 1 {
			t.Fatalf("Expected closest vector to have rowid 1, got %d", closestRowID)
		}

		if closestDistance != 0.0 {
			t.Fatalf("Expected exact match with distance 0, got %f", closestDistance)
		}
	})
}
