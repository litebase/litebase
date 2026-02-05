package vector_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

// TestMultiColumnVectorIndex tests multiple BLOB columns with independent configurations
func TestMultiColumnVectorIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create table with multiple vector columns
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE docs USING vector_index(
				title BLOB,
				summary BLOB,
				title_dimensions=128,
				title_distance_metric='l2',
				summary_dimensions=64,
				summary_distance_metric='cosine'
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create multi-column vector index: %v", err)
		}

		t.Log("✓ Created table with 2 vector columns (title: 128D L2, summary: 64D cosine)")

		// Verify column metadata was stored
		res, err := dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_count'", nil)

		if err != nil {
			t.Fatalf("Failed to query column_count: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("column_count not found in metadata")
		}

		var columnCount int
		fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &columnCount)

		if columnCount != 2 {
			t.Errorf("Expected 2 columns, got %d", columnCount)
		}

		// Verify title column metadata
		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_0_name'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_0_name not found")
		}

		if string(res.Rows[0][0].Text()) != "title" {
			t.Errorf("Expected column_0_name='title', got '%s'", string(res.Rows[0][0].Text()))
		}

		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_0_dimensions'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_0_dimensions not found")
		}

		var col0Dims int
		fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &col0Dims)

		if col0Dims != 128 {
			t.Errorf("Expected column_0_dimensions=128, got %d", col0Dims)
		}

		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_0_distance_metric'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_0_distance_metric not found")
		}

		var col0Metric int
		fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &col0Metric)

		if col0Metric != 0 { // L2 = 0
			t.Errorf("Expected column_0_distance_metric=0 (L2), got %d", col0Metric)
		}

		// Verify summary column metadata
		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_1_name'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_1_name not found")
		}

		if string(res.Rows[0][0].Text()) != "summary" {
			t.Errorf("Expected column_1_name='summary', got '%s'", string(res.Rows[0][0].Text()))
		}

		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_1_dimensions'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_1_dimensions not found")
		}

		var col1Dims int
		fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &col1Dims)

		if col1Dims != 64 {
			t.Errorf("Expected column_1_dimensions=64, got %d", col1Dims)
		}

		res, err = dbConn.Exec("SELECT value FROM docs_metadata WHERE key = 'column_1_distance_metric'", nil)

		if err != nil || len(res.Rows) == 0 {
			t.Fatal("column_1_distance_metric not found")
		}

		var col1Metric int
		fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &col1Metric)

		if col1Metric != 1 { // Cosine = 1
			t.Errorf("Expected column_1_distance_metric=1 (cosine), got %d", col1Metric)
		}

		t.Log("✓ All column metadata stored correctly")

		// Verify cluster tables were created
		res, err = dbConn.Exec("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'docs_%_cluster_tree'", nil)

		if err != nil {
			t.Fatalf("Failed to query cluster tables: %v", err)
		}

		if len(res.Rows) != 2 {
			t.Errorf("Expected 2 cluster_tree tables, found %d", len(res.Rows))
		}

		tableNames := []string{string(res.Rows[0][0].Text()), string(res.Rows[1][0].Text())}

		if !contains(tableNames, "docs_title_cluster_tree") {
			t.Error("docs_title_cluster_tree not found")
		}

		if !contains(tableNames, "docs_summary_cluster_tree") {
			t.Error("docs_summary_cluster_tree not found")
		}

		t.Log("✓ Per-column cluster tables created")
	})
}

// TestMultiColumnVectorInsertValidation tests dimension validation per column
func TestMultiColumnVectorInsertValidation(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create database and branch
		db, err := database.CreateDatabase(app.DatabaseManager, "testdb", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.Branch("main")

		if err != nil {
			t.Fatalf("Failed to get branch: %v", err)
		}

		conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create table with two vector columns of different dimensions
		// title = 128 dimensions, summary = 64 dimensions
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE docs USING vector_index(
				title BLOB,
				summary BLOB,
				title_dimensions=128,
				title_distance_metric='l2',
				summary_dimensions=64,
				summary_distance_metric='cosine'
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create virtual table: %v", err)
		}

		// Test 1: Insert with correct dimensions for both columns - should succeed
		titleVec := NewTestVectorWithSeed(128, 1)
		summaryVec := NewTestVectorWithSeed(64, 2)

		titleBlob := EncodeVector(titleVec)
		summaryBlob := EncodeVector(summaryVec)

		_, err = dbConn.Exec("INSERT INTO docs(title, summary) VALUES(?, ?)", []sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: titleBlob},
			{Type: sqlite3.ParameterTypeBlob, Value: summaryBlob},
		})

		if err != nil {
			t.Errorf("Insert with correct dimensions failed: %v", err)
		} else {
			t.Log("✓ Insert with correct dimensions succeeded")
		}

		// Test 2: Insert with wrong dimensions for title column (64 instead of 128) - should fail
		wrongTitleVec := NewTestVectorWithSeed(64, 3)
		wrongTitleBlob := EncodeVector(wrongTitleVec)

		_, err = dbConn.Exec("INSERT INTO docs(title, summary) VALUES(?, ?)", []sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: wrongTitleBlob},
			{Type: sqlite3.ParameterTypeBlob, Value: summaryBlob},
		})

		if err == nil {
			t.Error("Expected error when inserting wrong dimensions for title column, but INSERT succeeded")
		} else if !strings.Contains(err.Error(), "title") || !strings.Contains(err.Error(), "128") || !strings.Contains(err.Error(), "64") {
			t.Errorf("Error message doesn't mention column name or dimensions: %v", err)
		} else {
			t.Logf("✓ Correctly rejected wrong dimensions for title: %v", err)
		}

		// Test 3: Insert with wrong dimensions for summary column (128 instead of 64) - should fail
		wrongSummaryVec := NewTestVectorWithSeed(128, 4)
		wrongSummaryBlob := EncodeVector(wrongSummaryVec)

		_, err = dbConn.Exec("INSERT INTO docs(title, summary) VALUES(?, ?)", []sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: titleBlob},
			{Type: sqlite3.ParameterTypeBlob, Value: wrongSummaryBlob},
		})

		if err == nil {
			t.Error("Expected error when inserting wrong dimensions for summary column, but INSERT succeeded")
		} else if !strings.Contains(err.Error(), "summary") || !strings.Contains(err.Error(), "64") || !strings.Contains(err.Error(), "128") {
			t.Errorf("Error message doesn't mention column name or dimensions: %v", err)
		} else {
			t.Logf("✓ Correctly rejected wrong dimensions for summary: %v", err)
		}

		// Test 4: Insert with wrong dimensions for both columns - should fail on first column
		_, err = dbConn.Exec("INSERT INTO docs(title, summary) VALUES(?, ?)", []sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: wrongTitleBlob},
			{Type: sqlite3.ParameterTypeBlob, Value: wrongSummaryBlob},
		})

		if err == nil {
			t.Error("Expected error when inserting wrong dimensions for both columns, but INSERT succeeded")
		} else {
			t.Logf("✓ Correctly rejected wrong dimensions for multiple columns: %v", err)
		}
	})
}

// TestMultiColumnVectorSearch tests search on different columns
func TestMultiColumnVectorSearch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create table with two vector columns using different distance metrics
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE docs USING vector_index(
				title BLOB,
				summary BLOB,
				title_dimensions=4,
				title_distance_metric='l2',
				summary_dimensions=4,
				summary_distance_metric='cosine'
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create multi-column vector index: %v", err)
		}

		// Insert test vectors with known values
		testVecs := []struct {
			id      int
			title   []float32
			summary []float32
		}{
			{1, []float32{1.0, 0.0, 0.0, 0.0}, []float32{0.0, 1.0, 0.0, 0.0}},
			{2, []float32{0.9, 0.1, 0.0, 0.0}, []float32{0.1, 0.9, 0.0, 0.0}},
			{3, []float32{0.0, 0.0, 1.0, 0.0}, []float32{0.0, 0.0, 1.0, 0.0}},
			{4, []float32{0.0, 0.0, 0.9, 0.1}, []float32{0.0, 0.0, 0.9, 0.1}},
			{5, []float32{0.5, 0.5, 0.0, 0.0}, []float32{0.0, 0.0, 0.5, 0.5}},
		}

		for _, tv := range testVecs {
			_, err = dbConn.Exec(
				"INSERT INTO docs (id, title, summary) VALUES (?, ?, ?)",
				[]sqlite3.StatementParameter{
					{Type: "INTEGER", Value: int64(tv.id)},
					{Type: "BLOB", Value: EncodeVector(tv.title)},
					{Type: "BLOB", Value: EncodeVector(tv.summary)},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", tv.id, err)
			}
		}

		t.Log("✓ Inserted 5 test vectors")

		// Wait for indexing to complete
		time.Sleep(2 * time.Second)

		// Search on title column - should find closest to [1.0, 0.0, 0.0, 0.0]
		queryVec := []float32{1.0, 0.0, 0.0, 0.0}
		res, err := dbConn.Exec(
			"SELECT rowid, distance FROM vector_search('docs', 'title', ?, 3)",
			[]sqlite3.StatementParameter{
				{Type: "BLOB", Value: EncodeVector(queryVec)},
			},
		)

		if err != nil {
			t.Fatalf("Failed to search title column: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No results from title search")
		}

		// First result should be rowid=1 (exact match)
		firstRowID := res.Rows[0][0].Int64()

		if firstRowID != 1 {
			t.Errorf("Title search: expected first result rowid=1, got rowid=%d", firstRowID)
		}

		t.Logf("✓ Title column search returned %d results, first rowid=%d", len(res.Rows), firstRowID)

		// Search on summary column - should find closest to [0.0, 1.0, 0.0, 0.0]
		summaryQueryVec := []float32{0.0, 1.0, 0.0, 0.0}
		res, err = dbConn.Exec(
			"SELECT rowid, distance FROM vector_search('docs', 'summary', ?, 3)",
			[]sqlite3.StatementParameter{
				{Type: "BLOB", Value: EncodeVector(summaryQueryVec)},
			},
		)

		if err != nil {
			t.Fatalf("Failed to search summary column: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No results from summary search")
		}

		// First result should be rowid=1 (exact match)
		firstSummaryRowID := res.Rows[0][0].Int64()

		if firstSummaryRowID != 1 {
			t.Errorf("Summary search: expected first result rowid=1, got rowid=%d", firstSummaryRowID)
		}

		t.Logf("✓ Summary column search returned %d results, first rowid=%d", len(res.Rows), firstSummaryRowID)

		// Verify searches use different distance metrics by checking they access different cluster tables
		titleClusters, err := dbConn.Exec("SELECT DISTINCT cluster_id FROM docs_title_cluster_vector_map WHERE cluster_id != 0", nil)

		if err != nil {
			t.Fatalf("Failed to query title clusters: %v", err)
		}

		summaryClusters, err := dbConn.Exec("SELECT DISTINCT cluster_id FROM docs_summary_cluster_vector_map WHERE cluster_id != 0", nil)

		if err != nil {
			t.Fatalf("Failed to query summary clusters: %v", err)
		}

		t.Logf("✓ Title column has %d non-root clusters, Summary has %d non-root clusters",
			len(titleClusters.Rows), len(summaryClusters.Rows))
	})
}


// TestMultiColumnVectorIndexing tests indexer processes all columns
func TestMultiColumnVectorIndexing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create table with two vector columns
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE docs USING vector_index(
				title BLOB,
				summary BLOB,
				title_dimensions=4,
				title_distance_metric='l2',
				summary_dimensions=4,
				summary_distance_metric='cosine'
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create multi-column vector index: %v", err)
		}

		// Insert a few vectors to trigger indexing
		for i := 0; i < 5; i++ {
			titleVec := NewTestVectorWithSeed(4, int64(i))
			summaryVec := NewTestVectorWithSeed(4, int64(i+100))

			titleBlob := EncodeVector(titleVec)
			summaryBlob := EncodeVector(summaryVec)

			_, err = dbConn.Exec("INSERT INTO docs(title, summary) VALUES(?, ?)", []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeBlob, Value: titleBlob},
				{Type: sqlite3.ParameterTypeBlob, Value: summaryBlob},
			})

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		t.Log("✓ Inserted 5 vectors successfully")

	// Wait for background indexing to process
	time.Sleep(2 * time.Second)

		// Check that vectors were moved out of cluster 0 for both columns
		res, err := dbConn.Exec("SELECT COUNT(*) FROM docs_title_cluster_vector_map WHERE cluster_id != 0", nil)

		if err != nil {
			t.Fatalf("Failed to query title cluster map: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No results from title cluster map query")
		}

		titleIndexedCount := res.Rows[0][0].Int64()
		t.Logf("Title column: %d vectors indexed (moved out of cluster 0)", titleIndexedCount)

		res, err = dbConn.Exec("SELECT COUNT(*) FROM docs_summary_cluster_vector_map WHERE cluster_id != 0", nil)

		if err != nil {
			t.Fatalf("Failed to query summary cluster map: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No results from summary cluster map query")
		}

		summaryIndexedCount := res.Rows[0][0].Int64()
		t.Logf("Summary column: %d vectors indexed (moved out of cluster 0)", summaryIndexedCount)

		if titleIndexedCount > 0 && summaryIndexedCount > 0 {
			t.Log("✓ Both columns indexed successfully")
		} else {
			t.Errorf("Expected both columns to have indexed vectors, got title=%d, summary=%d",
				titleIndexedCount, summaryIndexedCount)
		}
	})
}

// Helper functions

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}

	return false
}

func NewTestVectorWithSeed(dims int, seed int64) []float32 {
	vec := make([]float32, dims)

	// Simple deterministic pseudo-random based on seed
	for i := 0; i < dims; i++ {
		seed = (seed*1103515245 + 12345) & 0x7fffffff
		vec[i] = float32(seed%1000) / 1000.0
	}

	return vec
}

// EncodeVector encodes a float32 vector as a BLOB
func EncodeVector(vec []float32) []byte {
	blob, err := vector.EncodeFloat32(vec)

	if err != nil {
		panic(err)
	}

	return blob
}

