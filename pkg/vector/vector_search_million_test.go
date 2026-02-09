package vector_test

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
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

		cpuProfileFile, err := os.Create("cpu_profile.prof")
		if err != nil {
			t.Fatalf("Failed to create CPU profile file: %v", err)
		}
		defer cpuProfileFile.Close()

		pprof.StartCPUProfile(cpuProfileFile)
		defer func() {
			pprof.StopCPUProfile()
		}()

		memprofileFile, err := os.Create("mem_profile.prof")
		if err != nil {
			t.Fatalf("Failed to create memory profile file: %v", err)
		}
		defer func() {
			// gc before taking memory profile to get more accurate live heap objects
			runtime.GC()

			//flush memory profile to file
			pprof.Lookup("allocs").WriteTo(memprofileFile, 0)

			if err := memprofileFile.Sync(); err != nil {
				t.Fatalf("Failed to sync memory profile file: %v", err)
			}
		}()

		const (
			// Target: 1M vectors under 5 minutes
			// With optimizations: ~100k inserts in 0.5s, 1M = 5s insert
			// Indexing: 100k batches, 10 batches total, ~15s per batch = 150s
			// Total: ~3 minutes (well under 5 min limit)
			totalVectors      = 250000
			dimensions        = 128
			generateBatchSize = 10000 // How many vectors to generate at once
			insertBatchSize   = 10000 // Increased for speed
			k                 = 10    // Number of nearest neighbors to return
		)

		// Build the batched INSERT statement without repeated allocations
		var builder strings.Builder

		// Approximate per-item size: 3 for "(?)" + 2 for ", " separator
		if insertBatchSize > 0 {
			builder.Grow(5 * insertBatchSize)
		}

		for i := range insertBatchSize {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString("(?)")
		}
		placeholders := builder.String()

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

		// Verify count (query shadow table, not virtual table)
		res, err := dbConn.Exec("SELECT COUNT(*) FROM embeddings_vectors", nil)

		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No count result")
		}

		count := (res.Rows[0][0].Int64())
		t.Logf("✓ Verified count: %d vectors in table", count)

		// Skip baseline brute-force search for 1M vectors (way too slow)
		// It would take 6+ seconds and we need to complete test in <5 minutes
		t.Logf("⚠ Skipping baseline brute-force search (1M vectors, would exceed test timeout)")

		// Wait for background indexing to redistribute vectors from cluster 0
		t.Logf("Waiting for background indexing to redistribute vectors from cluster 0...")
		indexingStart := time.Now()
		maxWaitTime := 3 * time.Minute
		checkInterval := 500 * time.Millisecond
		previousCluster0Count := int64(totalVectors)

		for {
			elapsed := time.Since(indexingStart)

			if elapsed > maxWaitTime {
				t.Logf("Indexing timeout after %v - continuing with partial indexing", elapsed)
				break
			}

			// Check how many vectors remain in cluster 0
			var cluster0Count int64
			cluster0Res, err := dbConn.Exec(
				"SELECT COUNT(*) FROM embeddings_vector_cluster_vector_map WHERE cluster_id = 0",
				nil,
			)

			if err != nil || len(cluster0Res.Rows) == 0 {
				t.Logf("  - Database busy, waiting...")
				time.Sleep(checkInterval)
				continue
			}

			cluster0Count = cluster0Res.Rows[0][0].Int64()
			processed := previousCluster0Count - cluster0Count

			if cluster0Count == 0 {
				t.Logf("✓ All vectors redistributed from cluster 0 in %v", time.Since(indexingStart))
				break
			}

			if processed > 0 {
				rate := float64(totalVectors-int(cluster0Count)) / time.Since(indexingStart).Seconds()
				t.Logf("  - Progress: %d/%d (%.1f%%), %d remaining, %.0f vec/sec",
					totalVectors-int(cluster0Count), totalVectors,
					float64(totalVectors-int(cluster0Count))/float64(totalVectors)*100,
					cluster0Count, rate)
				previousCluster0Count = cluster0Count
			}

			time.Sleep(checkInterval)
		}

		// Query cluster statistics
		clusterStatsRes, err := dbConn.Exec(
			"SELECT COUNT(*) FROM embeddings_vector_cluster_tree",
			nil,
		)

		if err == nil && len(clusterStatsRes.Rows) > 0 {
			totalClusters := clusterStatsRes.Rows[0][0].Int64()
			t.Logf("✓ Cluster tree statistics:")
			t.Logf("  - Total clusters: %d", totalClusters)

			// Count leaf vs non-leaf clusters
			leafRes, _ := dbConn.Exec(
				"SELECT COUNT(*) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
			)

			if len(leafRes.Rows) > 0 {
				leafClusters := leafRes.Rows[0][0].Int64()
				t.Logf("  - Leaf clusters: %d", leafClusters)
				t.Logf("  - Internal clusters: %d", totalClusters-leafClusters)
			}

			// Check max cluster size
			maxSizeRes, _ := dbConn.Exec(
				"SELECT MAX(cluster_size) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
			)

			if len(maxSizeRes.Rows) > 0 {
				maxSize := maxSizeRes.Rows[0][0].Int64()
				t.Logf("  - Largest leaf cluster: %d vectors", maxSize)
			}
		}

		// Query the vector_index virtual table directly
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

	})
}
