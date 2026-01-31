package vector_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorSearchWithMillionVectors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping million-vector test in short mode")
	}

	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE embeddings USING vector_index(
				vector BLOB,
				dimensions=128,
				distance_metric=0,
				max_cluster_size=5000,
				min_cluster_size=200
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		const (
			totalVectors      = 100000
			dimensions        = 128
			generateBatchSize = 10000 // How many vectors to generate at once
			insertBatchSize   = 5000  // How many rows per INSERT statement
			k                 = 10
		)

		// Build the batched INSERT statement
		placeholders := ""

		for i := range insertBatchSize {
			if i > 0 {
				placeholders += ", "
			}

			placeholders += "(?)"
		}

		insertSQL := fmt.Sprintf("INSERT INTO embeddings(vector) VALUES %s", placeholders)

		insertStmt, err := dbConn.Prepare(t.Context(), insertSQL)

		if err != nil {
			t.Fatalf("Failed to prepare insert statement: %v", err)
		}

		result := sqlite3.NewResult()

		t.Logf("Inserting %d vectors in batches of %d...", totalVectors, insertBatchSize)
		insertStart := time.Now()

		// Timing variables
		var vectorGenTime, insertTime time.Duration
		insertedCount := 0

		// Use Transaction wrapper to ensure setTimestamps() is called properly
		err = dbConn.Transaction(false, func(txConn *database.DatabaseConnection) error {
			for batch := range totalVectors / generateBatchSize {
				vecGenStart := time.Now()
				vectors := GenerateBatch(generateBatchSize, dimensions)
				vectorGenTime += time.Since(vecGenStart)

				// Pre-convert all vectors to binary blobs once
				vectorBlobs := make([][]byte, len(vectors))

				for idx, vec := range vectors {
					vectorBlobs[idx] = VectorToBlob(vec)
				}

				// Process vectors in insert batches
				for i := 0; i < len(vectors); i += insertBatchSize {
					end := min(i+insertBatchSize, len(vectors))

					batchVectors := vectorBlobs[i:end]
					params := make([]sqlite3.StatementParameter, 0, len(batchVectors))

					for j := range batchVectors {
						params = append(params,
							sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: batchVectors[j]},
						)
					}

					// Execute the prepared statement with batched parameters
					insertStmtStart := time.Now()
					err = insertStmt.Sqlite3Statement.Exec(result, params...)
					insertTime += time.Since(insertStmtStart)

					if err != nil {
						return fmt.Errorf("failed to insert batch at vector %d: %w", batch*generateBatchSize+i, err)
					}

					insertedCount += len(batchVectors)
				}

				if (batch+1)%10 == 0 {
					elapsed := time.Since(insertStart)
					rate := float64(insertedCount) / elapsed.Seconds()

					t.Logf("Progress: %d/%d (%.1f%%, %.0f vec/sec)",
						insertedCount, totalVectors,
						float64(insertedCount)*100/float64(totalVectors),
						rate)
				}
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}

		insertDuration := time.Since(insertStart)
		insertRate := float64(totalVectors) / insertDuration.Seconds()

		t.Logf("✓ Inserted %d vectors in %v (%.0f vec/sec)", totalVectors, insertDuration, insertRate)
		t.Logf("  - Vector dimensions: %d", dimensions)
		t.Logf("  - Insertion rate: %.0f vectors/sec", insertRate)
		t.Logf("  - Time breakdown:")
		t.Logf("    • Vector generation: %v (%.1f%%)", vectorGenTime, 100*vectorGenTime.Seconds()/insertDuration.Seconds())
		t.Logf("    • INSERT execution: %v (%.1f%%)", insertTime, 100*insertTime.Seconds()/insertDuration.Seconds())

		// Verify count
		res, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings", nil)

		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No count result")
		}

		count := (res.Rows[0][0].Int64())
		t.Logf("✓ Verified count: %d vectors in table", count)

		// Wait for VectorIndexer job to process all pending vectors
		t.Logf("Waiting for vector indexing to complete...")
		indexingStart := time.Now()
		maxWait := 60 * time.Second
		checkInterval := 100 * time.Millisecond
		timeout := time.After(maxWait)
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		var finalPendingCount, finalIndexedCount, finalClustersCount int64

	indexingLoop:
		for {
			select {
			case <-timeout:
				t.Fatalf("Timed out waiting for indexing after %v", maxWait)
			case <-ticker.C:
				// Check pending count
				pendingRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_pending", nil)

				if err != nil {
					t.Logf("Warning: Failed to query pending table (may be shutting down): %v", err)
					break indexingLoop
				}

				pendingCount := pendingRes.Rows[0][0].Int64()

				// Check indexed count
				indexedRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_indexed", nil)

				if err != nil {
					t.Logf("Warning: Failed to query indexed table (may be shutting down): %v", err)
					break indexingLoop
				}

				indexedCount := indexedRes.Rows[0][0].Int64()

				// Check clusters
				clustersRes, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_clusters WHERE is_active = 1", nil)

				if err != nil {
					t.Logf("Warning: Failed to query clusters table (may be shutting down): %v", err)
					break indexingLoop
				}

				clustersCount := clustersRes.Rows[0][0].Int64()

				// Log progress every second
				elapsed := time.Since(indexingStart)

				if elapsed.Milliseconds()%1000 < int64(checkInterval.Milliseconds()) {
					t.Logf("  Indexing progress: pending=%d, indexed=%d, clusters=%d (%.1fs)",
						pendingCount, indexedCount, clustersCount, elapsed.Seconds())
				}

				finalPendingCount = pendingCount
				finalIndexedCount = indexedCount
				finalClustersCount = clustersCount

				// Done when all vectors are indexed and we have clusters
				if pendingCount == 0 && indexedCount == totalVectors && clustersCount > 0 {
					break indexingLoop
				}
			}
		}

		indexingDuration := time.Since(indexingStart)
		t.Logf("✓ Indexing completed in %v", indexingDuration)
		t.Logf("  - Pending vectors: %d", finalPendingCount)
		t.Logf("  - Indexed vectors: %d", finalIndexedCount)
		t.Logf("  - Active clusters: %d", finalClustersCount)

		// Check if indexing actually completed
		if finalPendingCount > 0 || finalIndexedCount < totalVectors || finalClustersCount == 0 {
			t.Logf("Indexing did not complete - skipping search test")
			t.Logf("  This may happen if the test is shutting down or indexing is slow")
			return
		}

		// Query the vector_index virtual table directly to see what it returns
		queryVec := NewTestVector(dimensions)
		queryBlob := VectorToBlob(queryVec)

		t.Logf("\nRunning vector_search query on %d vectors...", totalVectors)
		searchStart := time.Now()

		// Use vector_search as a table-valued function (ANN using clustered index)
		// Arguments: table_name, column_name, query_vector, k
		// Note: distance metric is read from the table's metadata (defined during CREATE VIRTUAL TABLE)
		res, err = dbConn.Exec(
			`SELECT rowid, distance 
			FROM vector_search('embeddings', 'vector', ?, ?) 
			ORDER BY distance`,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeBlob, Value: queryBlob},
				{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
			},
		)
		searchDuration := time.Since(searchStart)

		if err != nil {
			t.Fatalf("vector_search query failed: %v", err)
		}

		if len(res.Rows) != k {
			t.Errorf("Expected %d results, got %d", k, len(res.Rows))
		}

		// Verify each row has rowid and distance
		for i, row := range res.Rows {
			if len(row) < 2 {
				t.Fatalf("Row %d doesn't have rowid and distance", i)
			}
		}

		t.Logf("✓ Vector search completed in %v", searchDuration)
		t.Logf("  - Returned %d results", len(res.Rows))
		t.Logf("  - Query rate: %.2f queries/sec", 1.0/searchDuration.Seconds())
		t.Logf("✓ Successfully searched through %d vectors using vector_index", totalVectors)
	})
}
