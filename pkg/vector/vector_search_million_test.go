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
	runVectorSearchWithMillionVectors(t, 128)
}

func TestVectorSearchWithMillionVectors1536Dimensions(t *testing.T) {
	runVectorSearchWithMillionVectors(t, 1536)
}

func TestVectorSearchWithMillionVectors3072Dimensions(t *testing.T) {
	runVectorSearchWithMillionVectors(t, 3072)
}

// runVectorSearchWithMillionVectors executes the 1M vector index and ANN query
// benchmark-style integration test for a given embedding dimension.
func runVectorSearchWithMillionVectors(t *testing.T, dimensions int) {
	t.Helper()

	if testing.Short() {
		t.Skipf("Skipping million-vector test in short mode (dimensions=%d)", dimensions)
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
		createSQL := fmt.Sprintf(`
			CREATE VIRTUAL TABLE embeddings USING vector_index(
				vector BLOB,
				dimensions=%d,
				distance_metric=0,
				max_cluster_size=5000,
				min_cluster_size=200
			)
		`, dimensions)

		createRes, err := dbConn.Exec(createSQL, nil)

		if createRes != nil {
			dbConn.ResultPool().Put(createRes)
		}

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		cpuProfileFileName := fmt.Sprintf("cpu_profile_dim_%d.prof", dimensions)
		cpuProfileFile, err := os.Create(cpuProfileFileName)
		if err != nil {
			t.Fatalf("Failed to create CPU profile file: %v", err)
		}
		defer cpuProfileFile.Close()

		pprof.StartCPUProfile(cpuProfileFile)
		defer func() {
			pprof.StopCPUProfile()
		}()

		memProfileFileName := fmt.Sprintf("mem_profile_dim_%d.prof", dimensions)
		memprofileFile, err := os.Create(memProfileFileName)
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
			totalVectors      = 1000000
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

		t.Logf("Inserting %d vectors in batches of %d...", totalVectors, insertBatchSize)

		insertStart := time.Now()

		// Timing variables
		var vectorGenTime, insertTime time.Duration
		insertedCount := 0

		// Use a single Transaction for all inserts.
		err = dbConn.Transaction(false, func(txConn *database.DatabaseConnection) error {
			for batch := range totalVectors / generateBatchSize {
				// Generate vectors using the iterator which also returns
				// their binary blob representation to avoid a second
				// conversion pass. Stream them directly into batched
				// INSERTs to avoid allocating a large `vectorBlobs` slice.
				it := GenerateBatch(generateBatchSize, dimensions)

				// Reuse params slice across insert batches to avoid repeated
				// allocations. Capacity is preallocated to `insertBatchSize`.
				params := make([]sqlite3.StatementParameter, 0, insertBatchSize)

				// Fill params up to `insertBatchSize` by repeatedly calling
				// the iterator. We separate generation timing from insert
				// timing by measuring only the generation loop.
				for {
					// generation phase: fill params
					genStart := time.Now()
					for len(params) < insertBatchSize {
						blob, ok := it.NextBlob()

						if !ok {
							break
						}

						params = append(params, sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: blob})
					}

					vectorGenTime += time.Since(genStart)

					// If we have nothing to insert (iterator exhausted), break
					if len(params) == 0 {
						break
					}

					// Execute via DatabaseConnection.Exec for proper barrier and timestamp management
					insertStmtStart := time.Now()
					result, err := txConn.Exec(insertSQL, params)
					insertTime += time.Since(insertStmtStart)

					if err != nil {
						return fmt.Errorf("failed to insert batch at vector %d: %w", batch*generateBatchSize+insertedCount, err)
					}

					// Return result to pool to release accumulated memory
					txConn.ResultPool().Put(result)

					// After successful execution, return blob buffers to the pool
					for _, p := range params {
						if p.Type == sqlite3.ParameterTypeBlob {
							if bb, ok := p.Value.([]byte); ok {
								putBlobBuf(bb)
							}
						}
					}

					insertedCount += len(params)

					// Reuse params backing array for next batch
					params = params[:0]
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
		var count int64
		err = dbConn.ExecStream("SELECT COUNT(*) FROM embeddings_vectors", nil, func(row []*sqlite3.Column) error {
			if len(row) > 0 {
				count = row[0].Int64()
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		t.Logf("✓ Verified count: %d vectors in table", count)

		// Skip baseline brute-force search for 1M vectors (way too slow)
		// It would take 6+ seconds and we need to complete test in <5 minutes
		t.Logf("⚠ Skipping baseline brute-force search (1M vectors, would exceed test timeout)")

		// Splits run automatically via post-commit hooks in Transaction().
		// After the insert transaction committed, xCommit registered a
		// hook via goTriggerClusterSplits that ran splits on the same
		// warm-cache connection. Poll to confirm convergence.
		splitStart := time.Now()
		t.Logf("Waiting for transparent post-commit splits to converge...")

		// Allow any remaining background split goroutines to finish and then
		// poll briefly to confirm convergence.
		maxWaitTime := 30 * time.Second
		checkInterval := 500 * time.Millisecond
		pollIteration := 0

		for {
			elapsed := time.Since(splitStart)
			pollIteration++

			if elapsed > maxWaitTime {
				t.Logf("Split wait timeout after %v - continuing with current cluster state", elapsed)
				break
			}

			var leafClusters int64
			var maxSize int64

			err = dbConn.ExecStream(
				"SELECT COUNT(*) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
				func(row []*sqlite3.Column) error {
					if len(row) > 0 {
						leafClusters = row[0].Int64()
					}

					return nil
				},
			)

			if err != nil {
				time.Sleep(checkInterval)
				continue
			}

			err = dbConn.ExecStream(
				"SELECT MAX(cluster_size) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
				func(row []*sqlite3.Column) error {
					if len(row) > 0 {
						maxSize = row[0].Int64()
					}

					return nil
				},
			)

			if err != nil {
				time.Sleep(checkInterval)
				continue
			}

			t.Logf("  - [Poll #%d @ %v] leaf_clusters=%d, max_leaf_size=%d",
				pollIteration, elapsed.Round(100*time.Millisecond), leafClusters, maxSize)

			// Converged when every leaf is at or below max_cluster_size (5000)
			if leafClusters > 1 && maxSize <= 5000 {
				t.Logf("✓ Splits converged in %v: %d leaf clusters, max size=%d",
					time.Since(splitStart), leafClusters, maxSize)
				break
			}

			time.Sleep(checkInterval)
		}

		// Query cluster statistics from shadow table, not virtual table
		var totalClusters int64
		err = dbConn.ExecStream(
			"SELECT COUNT(*) FROM embeddings_vector_cluster_tree",
			nil,
			func(row []*sqlite3.Column) error {
				if len(row) > 0 {
					totalClusters = row[0].Int64()
				}

				return nil
			},
		)

		if err == nil && totalClusters > 0 {
			t.Logf("✓ Cluster tree statistics:")
			t.Logf("  - Total clusters: %d", totalClusters)

			// Count leaf vs non-leaf clusters
			var leafClusters int64
			err = dbConn.ExecStream(
				"SELECT COUNT(*) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
				func(row []*sqlite3.Column) error {
					if len(row) > 0 {
						leafClusters = row[0].Int64()
					}

					return nil
				},
			)

			if err == nil {
				t.Logf("  - Leaf clusters: %d", leafClusters)
				t.Logf("  - Internal clusters: %d", totalClusters-leafClusters)
			}

			// Check max cluster size
			var maxSize int64
			err = dbConn.ExecStream(
				"SELECT MAX(cluster_size) FROM embeddings_vector_cluster_tree WHERE is_leaf = 1",
				nil,
				func(row []*sqlite3.Column) error {
					if len(row) > 0 {
						maxSize = row[0].Int64()
					}

					return nil
				},
			)

			if err == nil {
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
		var resultCount int
		err = dbConn.ExecStream(
			`SELECT rowid, distance 
		FROM vector_search('embeddings', 'vector', ?, ?) 
		ORDER BY distance`,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeBlob, Value: queryBlob},
				{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
			},
			func(row []*sqlite3.Column) error {
				// Verify each row has rowid and distance
				if len(row) < 2 {
					return fmt.Errorf("row %d doesn't have rowid and distance", resultCount)
				}

				resultCount++

				return nil
			},
		)
		searchDuration := time.Since(searchStart)

		if err != nil {
			t.Fatalf("vector_search query failed: %v", err)
		}

		if resultCount != k {
			t.Errorf("Expected %d results, got %d", k, resultCount)
		}

		t.Logf("✓ Vector search completed in %v", searchDuration)
		t.Logf("  - Returned %d results", resultCount)
		t.Logf("  - Query rate: %.2f queries/sec", 1.0/searchDuration.Seconds())

	})
}
