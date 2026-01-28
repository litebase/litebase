package database_test

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

func TestVectorExtensionRegistered(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test database
		testDb := test.MockDatabase(app)

		// Get a database connection
		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Verify vector_f32 function exists by trying to use it
		stmt, err := conn.GetConnection().Prepare(ctx, `SELECT vector_f32('[1.0, 2.0, 3.0]') as vec`)

		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result)

		if err != nil {
			t.Fatalf("vector_f32() function not available: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("vector_f32() returned no rows")
		}

		if len(result.Rows[0]) == 0 {
			t.Fatal("vector_f32() returned no columns")
		}

		vecColumn := result.Rows[0][0]

		if vecColumn.ColumnType != sqlite3.ColumnTypeBlob {
			t.Fatalf("Expected blob type, got %v", vecColumn.ColumnType)
		}

		blob := vecColumn.ColumnValue

		// Verify the blob structure (version byte + dimensions + data)
		if blob[0] != 0x01 { // VectorVersion1
			t.Errorf("Expected version byte 0x01, got 0x%02x", blob[0])
		}

		// Expected: 1 (version) + 1 (type) + 4 (dimensions as uint32) + 12 (3 * 4 bytes for float32)
		expectedLen := 1 + 1 + 4 + 12

		if len(blob) != expectedLen {
			t.Errorf("Expected blob length %d, got %d", expectedLen, len(blob))
		}

		t.Logf("✓ Vector extension successfully loaded and working!")
		t.Logf("  - Returned %d bytes", len(blob))
		t.Logf("  - Version: 0x%02x", blob[0])
	})
}

func TestVectorStorageAndRetrieval(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create table with vector column
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE embeddings (
				id INTEGER PRIMARY KEY,
				name TEXT,
				vector BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test vectors
		testVectors := []struct {
			id     int
			name   string
			vector string
		}{
			{1, "vec1", "[1.0, 0.0, 0.0]"},
			{2, "vec2", "[0.0, 1.0, 0.0]"},
			{3, "vec3", "[0.0, 0.0, 1.0]"},
			{4, "vec4", "[0.707, 0.707, 0.0]"},
		}

		for _, tv := range testVectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO embeddings (id, name, vector) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(tv.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(tv.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(tv.vector)},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %s: %v", tv.name, err)
			}
		}

		// Retrieve and verify vectors
		stmt, err := conn.GetConnection().Prepare(ctx, `SELECT id, name, vector FROM embeddings ORDER BY id`)

		if err != nil {
			t.Fatalf("Failed to prepare select: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result)

		if err != nil {
			t.Fatalf("Failed to execute select: %v", err)
		}

		if len(result.Rows) != len(testVectors) {
			t.Fatalf("Expected %d rows, got %d", len(testVectors), len(result.Rows))
		}

		// Verify each vector
		for i, row := range result.Rows {
			if len(row) < 3 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			id := binary.LittleEndian.Uint64(row[0].ColumnValue)
			name := string(row[1].ColumnValue)
			blob := row[2].ColumnValue

			if int(id) != testVectors[i].id {
				t.Errorf("Row %d: expected id %d, got %d", i, testVectors[i].id, id)
			}

			if name != testVectors[i].name {
				t.Errorf("Row %d: expected name %s, got %s", i, testVectors[i].name, name)
			}

			// Decode and verify vector
			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatalf("Row %d: failed to decode vector: %v", i, err)
			}

			if vecBlob.Dimensions != 3 {
				t.Errorf("Row %d: expected 3 dimensions, got %d", i, vecBlob.Dimensions)
			}
		}

		t.Logf("✓ Vector storage and retrieval working!")
		t.Logf("  - Stored %d vectors", len(testVectors))
		t.Logf("  - All vectors retrieved correctly")
	})
}

func TestVectorDistanceCalculations(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create test vectors
		vec1JSON := "[1.0, 0.0, 0.0]"
		vec2JSON := "[0.0, 1.0, 0.0]"
		vec3JSON := "[1.0, 1.0, 0.0]"

		// Get vector blobs from database
		stmt, err := conn.GetConnection().Prepare(ctx, `SELECT vector_f32(?) as v1, vector_f32(?) as v2, vector_f32(?) as v3`)

		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(vec1JSON)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(vec2JSON)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(vec3JSON)},
		)

		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		if len(result.Rows) == 0 || len(result.Rows[0]) < 3 {
			t.Fatal("Failed to get vector blobs")
		}

		vec1Blob, err := vector.ParseVectorBlob(result.Rows[0][0].ColumnValue)

		if err != nil {
			t.Fatalf("Failed to decode vec1: %v", err)
		}

		vec2Blob, err := vector.ParseVectorBlob(result.Rows[0][1].ColumnValue)

		if err != nil {
			t.Fatalf("Failed to decode vec2: %v", err)
		}

		vec3Blob, err := vector.ParseVectorBlob(result.Rows[0][2].ColumnValue)

		if err != nil {
			t.Fatalf("Failed to decode vec3: %v", err)
		}

		// Test L2 distance
		// [1,0,0] and [0,1,0] are orthogonal, distance should be sqrt(2)
		distL2, err := vector.DistanceL2(vec1Blob, vec2Blob)

		if err != nil {
			t.Fatalf("Failed to compute L2 distance: %v", err)
		}

		expectedL2 := math.Sqrt(2.0)

		if math.Abs(distL2-expectedL2) > 0.0001 {
			t.Errorf("L2 distance: expected %.4f, got %.4f", expectedL2, distL2)
		}

		// Test cosine distance
		// Orthogonal vectors have cosine similarity 0, distance 1
		distCosine, err := vector.DistanceCosine(vec1Blob, vec2Blob)

		if err != nil {
			t.Fatalf("Failed to compute cosine distance: %v", err)
		}

		if math.Abs(distCosine-1.0) > 0.0001 {
			t.Errorf("Cosine distance: expected 1.0, got %.4f", distCosine)
		}

		// Test dot product
		// [1,0,0] · [0,1,0] = 0, so distance (negative dot) should be 0
		distDot, err := vector.DistanceDot(vec1Blob, vec2Blob)

		if err != nil {
			t.Fatalf("Failed to compute dot product distance: %v", err)
		}

		if math.Abs(distDot) > 0.0001 {
			t.Errorf("Dot product distance: expected 0.0, got %.4f", distDot)
		}

		// Test with vec3 [1,1,0] and vec1 [1,0,0]
		// L2 distance should be 1.0
		distL2_v3v1, err := vector.DistanceL2(vec3Blob, vec1Blob)

		if err != nil {
			t.Fatalf("Failed to compute L2 distance (vec3-vec1): %v", err)
		}

		if math.Abs(distL2_v3v1-1.0) > 0.0001 {
			t.Errorf("L2 distance (vec3-vec1): expected 1.0, got %.4f", distL2_v3v1)
		}

		// Dot product [1,1,0] · [1,0,0] = 1, so distance should be -1
		distDot_v3v1, err := vector.DistanceDot(vec3Blob, vec1Blob)

		if err != nil {
			t.Fatalf("Failed to compute dot distance (vec3-vec1): %v", err)
		}

		if math.Abs(distDot_v3v1-(-1.0)) > 0.0001 {
			t.Errorf("Dot distance (vec3-vec1): expected -1.0, got %.4f", distDot_v3v1)
		}

		t.Logf("✓ Vector distance calculations working!")
		t.Logf("  - L2 distance (orthogonal): %.4f", distL2)
		t.Logf("  - Cosine distance (orthogonal): %.4f", distCosine)
		t.Logf("  - Dot distance (orthogonal): %.4f", distDot)
		t.Logf("  - All distance metrics verified")
	})
}

func TestVectorScanVirtualTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create a table with vectors
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				name TEXT,
				embedding BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create items table: %v", err)
		}

		// Insert test data
		vectors := []struct {
			id   int
			name string
			vec  string
		}{
			{1, "item1", "[1.0, 0.0, 0.0, 0.0]"},
			{2, "item2", "[0.9, 0.1, 0.0, 0.0]"},
			{3, "item3", "[0.0, 1.0, 0.0, 0.0]"},
			{4, "item4", "[0.0, 0.0, 1.0, 0.0]"},
			{5, "item5", "[0.8, 0.2, 0.0, 0.0]"},
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.vec)},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %s: %v", v.name, err)
			}
		}

		// Perform a k-NN search for vectors similar to [1.0, 0.0, 0.0, 0.0]
		// Should return item1, item2, item5 as they are closest
		queryVector := "[1.0, 0.0, 0.0, 0.0]"
		k := 3

		// Call vector_scan as a table-valued function:
		// - Arguments are: table_name, column_name, query_vector, k, metric
		// - Returns: rowid (INTEGER), distance (REAL)
		// - Using hidden column approach where arguments map to hidden columns
		stmt, err := conn.GetConnection().Prepare(ctx, `
			SELECT rowid, distance
			FROM vector_scan('items', 'embedding', vector_f32(?), ?, 'l2')
			ORDER BY distance
		`)

		if err != nil {
			t.Fatalf("Failed to prepare k-NN query: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(queryVector)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
		)

		if err != nil {
			t.Fatalf("Failed to execute k-NN query: %v", err)
		}

		if len(result.Rows) != k {
			t.Errorf("Expected %d results, got %d", k, len(result.Rows))
		}

		// Verify results are ordered by distance
		var lastDistance float64

		for i, row := range result.Rows {
			if len(row) < 2 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			rowid := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
			distance := math.Float64frombits(binary.LittleEndian.Uint64(row[1].ColumnValue))

			if i > 0 && distance < lastDistance {
				t.Errorf("Results not ordered by distance: row %d has distance %.4f < previous %.4f",
					i, distance, lastDistance)
			}

			lastDistance = distance

			t.Logf("  Result %d: rowid=%d, distance=%.4f", i+1, rowid, distance)
		}

		// The first result should be item1 (exact match, distance ~0)
		if len(result.Rows) > 0 {
			firstRowID := int64(binary.LittleEndian.Uint64(result.Rows[0][0].ColumnValue))
			firstDistance := math.Float64frombits(binary.LittleEndian.Uint64(result.Rows[0][1].ColumnValue))

			if firstRowID != 1 {
				t.Errorf("Expected first result to be item1 (rowid=1), got rowid=%d", firstRowID)
			}

			if firstDistance > 0.0001 {
				t.Errorf("Expected first result distance ~0, got %.4f", firstDistance)
			}
		}

		t.Logf("✓ Vector scan virtual table working!")
		t.Logf("  - Created virtual table successfully")
		t.Logf("  - k-NN search returned %d results", len(result.Rows))
		t.Logf("  - Results properly ordered by distance")
	})
}

func TestVectorScanCosineMetric(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create a table with vectors
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				name TEXT,
				embedding BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create items table: %v", err)
		}

		// Insert test data with vectors of different angles
		// Using normalized vectors to better test cosine similarity
		vectors := []struct {
			id   int
			name string
			vec  string
		}{
			{1, "item1", "[1.0, 0.0, 0.0, 0.0]"},     // Same direction as query
			{2, "item2", "[0.707, 0.707, 0.0, 0.0]"}, // 45 degrees
			{3, "item3", "[0.0, 1.0, 0.0, 0.0]"},     // 90 degrees (orthogonal)
			{4, "item4", "[-1.0, 0.0, 0.0, 0.0]"},    // 180 degrees (opposite)
			{5, "item5", "[0.866, 0.5, 0.0, 0.0]"},   // 30 degrees
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.vec)},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %s: %v", v.name, err)
			}
		}

		// Perform a k-NN search using cosine metric
		// Query vector: [1.0, 0.0, 0.0, 0.0]
		// Expected order by cosine distance:
		// 1. item1 (0 degrees, distance ~0)
		// 2. item5 (30 degrees, distance ~0.134)
		// 3. item2 (45 degrees, distance ~0.293)
		queryVector := "[1.0, 0.0, 0.0, 0.0]"
		k := 3

		stmt, err := conn.GetConnection().Prepare(ctx, `
			SELECT rowid, distance
			FROM vector_scan('items', 'embedding', vector_f32(?), ?, 'cosine')
			ORDER BY distance
		`)

		if err != nil {
			t.Fatalf("Failed to prepare k-NN query: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(queryVector)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
		)

		if err != nil {
			t.Fatalf("Failed to execute k-NN query: %v", err)
		}

		if len(result.Rows) != k {
			t.Errorf("Expected %d results, got %d", k, len(result.Rows))
		}

		// Verify results are ordered by distance
		var lastDistance float64

		for i, row := range result.Rows {
			if len(row) < 2 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			rowid := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
			distance := math.Float64frombits(binary.LittleEndian.Uint64(row[1].ColumnValue))

			if i > 0 && distance < lastDistance {
				t.Errorf("Results not ordered by distance: row %d has distance %.4f < previous %.4f",
					i, distance, lastDistance)
			}

			lastDistance = distance

			t.Logf("  Result %d: rowid=%d, distance=%.4f", i+1, rowid, distance)
		}

		// The first result should be item1 (exact match, cosine distance ~0)
		if len(result.Rows) > 0 {
			firstRowID := int64(binary.LittleEndian.Uint64(result.Rows[0][0].ColumnValue))
			firstDistance := math.Float64frombits(binary.LittleEndian.Uint64(result.Rows[0][1].ColumnValue))

			if firstRowID != 1 {
				t.Errorf("Expected first result to be item1 (rowid=1), got rowid=%d", firstRowID)
			}

			if firstDistance > 0.0001 {
				t.Errorf("Expected first result distance ~0, got %.4f", firstDistance)
			}
		}

		t.Logf("✓ Vector scan with cosine metric working!")
		t.Logf("  - k-NN search returned %d results", len(result.Rows))
		t.Logf("  - Results properly ordered by cosine distance")
	})
}

func TestVectorScanDotProductMetric(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create a table with vectors
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				name TEXT,
				embedding BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create items table: %v", err)
		}

		// Insert test data
		// For dot product, higher values mean more similar (less distance)
		vectors := []struct {
			id   int
			name string
			vec  string
		}{
			{1, "item1", "[1.0, 0.0, 0.0, 0.0]"},  // dot = 1.0, distance = -1.0
			{2, "item2", "[0.5, 0.5, 0.0, 0.0]"},  // dot = 0.5, distance = -0.5
			{3, "item3", "[0.0, 1.0, 0.0, 0.0]"},  // dot = 0.0, distance = 0.0
			{4, "item4", "[-1.0, 0.0, 0.0, 0.0]"}, // dot = -1.0, distance = 1.0
			{5, "item5", "[0.8, 0.2, 0.0, 0.0]"},  // dot = 0.8, distance = -0.8
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.vec)},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %s: %v", v.name, err)
			}
		}

		// Perform a k-NN search using dot product metric
		// Query vector: [1.0, 0.0, 0.0, 0.0]
		// Expected order by dot product distance (negative dot product):
		// 1. item1 (dot=1.0, distance=-1.0)
		// 2. item5 (dot=0.8, distance=-0.8)
		// 3. item2 (dot=0.5, distance=-0.5)
		queryVector := "[1.0, 0.0, 0.0, 0.0]"
		k := 3

		stmt, err := conn.GetConnection().Prepare(ctx, `
			SELECT rowid, distance
			FROM vector_scan('items', 'embedding', vector_f32(?), ?, 'dot')
			ORDER BY distance
		`)

		if err != nil {
			t.Fatalf("Failed to prepare k-NN query: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(queryVector)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
		)

		if err != nil {
			t.Fatalf("Failed to execute k-NN query: %v", err)
		}

		if len(result.Rows) != k {
			t.Errorf("Expected %d results, got %d", k, len(result.Rows))
		}

		// Verify results are ordered by distance (lowest first)
		var lastDistance float64 = math.Inf(-1)

		for i, row := range result.Rows {
			if len(row) < 2 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			rowid := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
			distance := math.Float64frombits(binary.LittleEndian.Uint64(row[1].ColumnValue))

			if i > 0 && distance < lastDistance {
				t.Errorf("Results not ordered by distance: row %d has distance %.4f < previous %.4f",
					i, distance, lastDistance)
			}

			lastDistance = distance

			t.Logf("  Result %d: rowid=%d, distance=%.4f", i+1, rowid, distance)
		}

		// The first result should be item1 (highest dot product, lowest distance)
		if len(result.Rows) > 0 {
			firstRowID := int64(binary.LittleEndian.Uint64(result.Rows[0][0].ColumnValue))
			firstDistance := math.Float64frombits(binary.LittleEndian.Uint64(result.Rows[0][1].ColumnValue))

			if firstRowID != 1 {
				t.Errorf("Expected first result to be item1 (rowid=1), got rowid=%d", firstRowID)
			}

			// For dot product with identical vectors, distance should be -1.0
			if math.Abs(firstDistance-(-1.0)) > 0.0001 {
				t.Errorf("Expected first result distance ~-1.0, got %.4f", firstDistance)
			}
		}

		t.Logf("✓ Vector scan with dot product metric working!")
		t.Logf("  - k-NN search returned %d results", len(result.Rows))
		t.Logf("  - Results properly ordered by dot product distance")
	})
}
