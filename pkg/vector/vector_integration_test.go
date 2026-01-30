package vector_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

// NewTestVector creates a random float32 slice for testing
func NewTestVector(dim int) []float32 {
	vec := make([]float32, dim)

	for i := range vec {
		vec[i] = rand.Float32()
	}

	return vec
}

// GenerateBatch creates multiple test vectors
func GenerateBatch(count, dim int) [][]float32 {
	batch := make([][]float32, count)

	for i := range count {
		vec := make([]float32, dim)

		for j := range vec {
			vec[j] = rand.Float32()
		}

		batch[i] = vec
	}

	return batch
}

// VectorToJSON converts a float32 slice to JSON array string
func VectorToJSON(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}

	var builder strings.Builder
	// Pre-allocate capacity: "[" + n*("%f" ~10 chars + ", ") + "]"
	builder.Grow(1 + len(vec)*13 + 1)

	builder.WriteString("[")

	for i, v := range vec {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteString(fmt.Sprintf("%f", v))
	}

	builder.WriteString("]")

	return builder.String()
}

// VectorToBlob converts a float32 slice to binary vector blob format
// Format: [version(1) | type(1) | dimensions(4) | data(n*4)]
func VectorToBlob(vec []float32) []byte {
	const (
		vectorVersion1 = 0x01
		vectorTypeF32  = 0x01
	)

	// Calculate total size: version + type + dimensions + data
	blobSize := 1 + 1 + 4 + len(vec)*4
	blob := make([]byte, blobSize)

	// Version byte
	blob[0] = vectorVersion1

	// Type byte (float32)
	blob[1] = vectorTypeF32

	// Dimensions (uint32, little-endian)
	binary.LittleEndian.PutUint32(blob[2:6], uint32(len(vec)))

	// Vector data (float32 values)
	offset := 6

	for _, v := range vec {
		bits := math.Float32bits(v)
		binary.LittleEndian.PutUint32(blob[offset:offset+4], bits)
		offset += 4
	}

	return blob
}

func TestVectorHelperFunctions(t *testing.T) {
	// Test NewTestVector
	vec := NewTestVector(4)

	if len(vec) != 4 {
		t.Fatalf("Expected vector with 4 dimensions, got %d", len(vec))
	}

	// Test GenerateBatch
	batch := GenerateBatch(5, 3)

	if len(batch) != 5 {
		t.Fatalf("Expected batch of 5 vectors, got %d", len(batch))
	}

	for i, v := range batch {
		if len(v) != 3 {
			t.Fatalf("Batch vector %d: expected 3 dimensions, got %d", i, len(v))
		}
	}

	// Test VectorToJSON
	testVec := []float32{1.0, 0.5, 0.25}
	jsonStr := VectorToJSON(testVec)
	expected := "[1.000000, 0.500000, 0.250000]"

	if jsonStr != expected {
		t.Errorf("VectorToJSON: expected %s, got %s", expected, jsonStr)
	}

	// Test with the helpers in a real scenario
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		// Create table
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE test_vectors (
				id INTEGER PRIMARY KEY,
				vector BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Use helper to create and insert vectors
		vectors := GenerateBatch(3, 4)

		for i, vec := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO test_vectors (id, vector) VALUES (?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(i + 1)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(vec))},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		t.Logf("✓ Successfully used helper functions to create and insert %d vectors", len(vectors))
	})
}

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
			vec  []float32
		}{
			{1, "item1", []float32{1.0, 0.0, 0.0, 0.0}},
			{2, "item2", []float32{0.9, 0.1, 0.0, 0.0}},
			{3, "item3", []float32{0.0, 1.0, 0.0, 0.0}},
			{4, "item4", []float32{0.0, 0.0, 1.0, 0.0}},
			{5, "item5", []float32{0.8, 0.2, 0.0, 0.0}},
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(v.vec))},
				})

			if err != nil {
				t.Fatalf("Failed to insert vector %s: %v", v.name, err)
			}
		}

		// Perform a k-NN search for vectors similar to [1.0, 0.0, 0.0, 0.0]
		// Should return item1, item2, item5 as they are closest
		queryVector := VectorToJSON([]float32{1.0, 0.0, 0.0, 0.0})
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
			vec  []float32
		}{
			{1, "item1", []float32{1.0, 0.0, 0.0, 0.0}},     // Same direction as query
			{2, "item2", []float32{0.707, 0.707, 0.0, 0.0}}, // 45 degrees
			{3, "item3", []float32{0.0, 1.0, 0.0, 0.0}},     // 90 degrees (orthogonal)
			{4, "item4", []float32{-1.0, 0.0, 0.0, 0.0}},    // 180 degrees (opposite)
			{5, "item5", []float32{0.866, 0.5, 0.0, 0.0}},   // 30 degrees
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(v.vec))},
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
		queryVector := VectorToJSON([]float32{1.0, 0.0, 0.0, 0.0})
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
			vec  []float32
		}{
			{1, "item1", []float32{1.0, 0.0, 0.0, 0.0}},  // dot = 1.0, distance = -1.0
			{2, "item2", []float32{0.5, 0.5, 0.0, 0.0}},  // dot = 0.5, distance = -0.5
			{3, "item3", []float32{0.0, 1.0, 0.0, 0.0}},  // dot = 0.0, distance = 0.0
			{4, "item4", []float32{-1.0, 0.0, 0.0, 0.0}}, // dot = -1.0, distance = 1.0
			{5, "item5", []float32{0.8, 0.2, 0.0, 0.0}},  // dot = 0.8, distance = -0.8
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, name, embedding) VALUES (?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(v.vec))},
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
		queryVector := VectorToJSON([]float32{1.0, 0.0, 0.0, 0.0})
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
		var lastDistance = math.Inf(-1)

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

func TestVectorScanWithFiltering(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create a table with vectors and additional columns for filtering/sorting
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE products (
				id INTEGER PRIMARY KEY,
				name TEXT,
				category TEXT,
				price REAL,
				in_stock INTEGER,
				embedding BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create products table: %v", err)
		}

		// Insert test data with different categories and prices
		vectors := []struct {
			id       int
			name     string
			category string
			price    float64
			inStock  int
			vec      []float32
		}{
			{1, "Product A", "electronics", 100.0, 1, []float32{1.0, 0.0, 0.0, 0.0}},
			{2, "Product B", "electronics", 200.0, 1, []float32{0.9, 0.1, 0.0, 0.0}},
			{3, "Product C", "books", 50.0, 1, []float32{0.8, 0.2, 0.0, 0.0}},
			{4, "Product D", "electronics", 150.0, 0, []float32{0.85, 0.15, 0.0, 0.0}},
			{5, "Product E", "books", 30.0, 1, []float32{0.7, 0.3, 0.0, 0.0}},
		}

		for _, v := range vectors {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO products (id, name, category, price, in_stock, embedding) VALUES (?, ?, ?, ?, ?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.name)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(v.category)},
					{Type: sqlite3.ParameterTypeFloat, Value: v.price},
					{Type: sqlite3.ParameterTypeInteger, Value: int64(v.inStock)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(v.vec))},
				})

			if err != nil {
				t.Fatalf("Failed to insert product %s: %v", v.name, err)
			}
		}

		// Perform k-NN search with filtering: only electronics that are in stock
		// Then sort by price (ascending) among similar items
		queryVector := VectorToJSON([]float32{1.0, 0.0, 0.0, 0.0})
		k := 10 // Request more than we'll get after filtering

		stmt, err := conn.GetConnection().Prepare(ctx, `
			SELECT p.id, p.name, p.category, p.price, v.distance
			FROM products p
			JOIN vector_scan('products', 'embedding', vector_f32(?), ?, 'l2') v
			  ON p.rowid = v.rowid
			WHERE p.category = 'electronics' AND p.in_stock = 1
			ORDER BY p.price ASC
		`)

		if err != nil {
			t.Fatalf("Failed to prepare filtered k-NN query: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(queryVector)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
		)

		if err != nil {
			t.Fatalf("Failed to execute filtered k-NN query: %v", err)
		}

		// Should only return Product A and Product B (electronics in stock)
		// Sorted by price: A (100) then B (200)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 results after filtering, got %d", len(result.Rows))
		}

		// Verify results are sorted by price
		for i, row := range result.Rows {
			if len(row) < 5 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			id := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
			name := string(row[1].ColumnValue)
			category := string(row[2].ColumnValue)
			price := math.Float64frombits(binary.LittleEndian.Uint64(row[3].ColumnValue))
			distance := math.Float64frombits(binary.LittleEndian.Uint64(row[4].ColumnValue))

			t.Logf("  Result %d: id=%d, name=%s, category=%s, price=%.2f, distance=%.4f",
				i+1, id, name, category, price, distance)

			// Verify filtering worked
			if category != "electronics" {
				t.Errorf("Row %d: expected category 'electronics', got '%s'", i, category)
			}
		}

		// Verify price ordering
		if len(result.Rows) >= 2 {
			price1 := math.Float64frombits(binary.LittleEndian.Uint64(result.Rows[0][3].ColumnValue))
			price2 := math.Float64frombits(binary.LittleEndian.Uint64(result.Rows[1][3].ColumnValue))

			if price1 > price2 {
				t.Errorf("Results not sorted by price: %.2f > %.2f", price1, price2)
			}
		}

		t.Logf("✓ Vector scan with filtering and sorting working!")
		t.Logf("  - Filtered by category and stock status")
		t.Logf("  - Sorted by price within similar items")
		t.Logf("  - Returned %d results", len(result.Rows))
	})
}

func TestVectorScanWithJoin(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create two tables: one with vectors and one with metadata
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE embeddings (
				id INTEGER PRIMARY KEY,
				vector BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create embeddings table: %v", err)
		}

		_, err = conn.GetConnection().Exec(`
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				embedding_id INTEGER,
				title TEXT,
				description TEXT,
				FOREIGN KEY (embedding_id) REFERENCES embeddings(id)
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create items table: %v", err)
		}

		// Insert embeddings
		embeddings := []struct {
			id  int
			vec []float32
		}{
			{1, []float32{1.0, 0.0, 0.0, 0.0}},
			{2, []float32{0.9, 0.1, 0.0, 0.0}},
			{3, []float32{0.0, 1.0, 0.0, 0.0}},
			{4, []float32{0.8, 0.2, 0.0, 0.0}},
			{5, []float32{0.0, 0.0, 1.0, 0.0}},
		}

		for _, e := range embeddings {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO embeddings (id, vector) VALUES (?, vector_f32(?))`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(e.id)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(VectorToJSON(e.vec))},
				})

			if err != nil {
				t.Fatalf("Failed to insert embedding %d: %v", e.id, err)
			}
		}

		// Insert items that reference embeddings
		items := []struct {
			id          int
			embeddingID int
			title       string
			description string
		}{
			{1, 1, "Red Apple", "A fresh red apple"},
			{2, 2, "Red Tomato", "A ripe red tomato"},
			{3, 3, "Blue Car", "A fast blue car"},
			{4, 4, "Red Strawberry", "A sweet red strawberry"},
			{5, 5, "Blue Ocean", "A beautiful blue ocean"},
		}

		for _, item := range items {
			_, err = conn.GetConnection().Exec(
				`INSERT INTO items (id, embedding_id, title, description) VALUES (?, ?, ?, ?)`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: int64(item.id)},
					{Type: sqlite3.ParameterTypeInteger, Value: int64(item.embeddingID)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(item.title)},
					{Type: sqlite3.ParameterTypeText, Value: []byte(item.description)},
				})

			if err != nil {
				t.Fatalf("Failed to insert item %s: %v", item.title, err)
			}
		}

		// Perform k-NN search on embeddings and join with items table
		// Search for items similar to [1.0, 0.0, 0.0, 0.0]
		queryVector := VectorToJSON([]float32{1.0, 0.0, 0.0, 0.0})
		k := 3

		stmt, err := conn.GetConnection().Prepare(ctx, `
			SELECT i.id, i.title, i.description, v.distance
			FROM vector_scan('embeddings', 'vector', vector_f32(?), ?, 'l2') v
			JOIN embeddings e ON e.rowid = v.rowid
			JOIN items i ON i.embedding_id = e.id
			ORDER BY v.distance
		`)

		if err != nil {
			t.Fatalf("Failed to prepare JOIN query: %v", err)
		}

		result := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(result,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(queryVector)},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
		)

		if err != nil {
			t.Fatalf("Failed to execute JOIN query: %v", err)
		}

		if len(result.Rows) != k {
			t.Errorf("Expected %d results, got %d", k, len(result.Rows))
		}

		// Verify results and ordering
		var lastDistance float64

		for i, row := range result.Rows {
			if len(row) < 4 {
				t.Fatalf("Row %d has insufficient columns", i)
			}

			id := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
			title := string(row[1].ColumnValue)
			description := string(row[2].ColumnValue)
			distance := math.Float64frombits(binary.LittleEndian.Uint64(row[3].ColumnValue))

			if i > 0 && distance < lastDistance {
				t.Errorf("Results not ordered by distance: row %d has distance %.4f < previous %.4f",
					i, distance, lastDistance)
			}

			lastDistance = distance

			t.Logf("  Result %d: id=%d, title=%s, description=%s, distance=%.4f",
				i+1, id, title, description, distance)
		}

		// First result should be "Red Apple" (exact match)
		if len(result.Rows) > 0 {
			firstTitle := string(result.Rows[0][1].ColumnValue)

			if firstTitle != "Red Apple" {
				t.Errorf("Expected first result to be 'Red Apple', got '%s'", firstTitle)
			}
		}

		t.Logf("✓ Vector scan with JOIN working!")
		t.Logf("  - Joined vector_scan with embeddings and items tables")
		t.Logf("  - Retrieved item metadata for nearest neighbors")
		t.Logf("  - Returned %d results", len(result.Rows))
	})
}

func TestVectorScanPerformanceWithMillionProducts(t *testing.T) {
	// Skip if not running long tests
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		ctx := context.Background()

		// Create a table with vectors and metadata
		_, err = conn.GetConnection().Exec(`
			CREATE TABLE products (
				id INTEGER PRIMARY KEY,
				name TEXT,
				category TEXT,
				price REAL,
				embedding BLOB
			)`, nil)

		if err != nil {
			t.Fatalf("Failed to create products table: %v", err)
		}

		t.Logf("Generating 1 million product vectors...")

		// Generate 1 million 128-dimensional vectors
		const totalProducts = 1_000_000
		const vectorDim = 128
		const generateBatchSize = 10_000 // How many vectors to generate at once
		const insertBatchSize = 1000     // How many rows per INSERT statement

		startGenerate := time.Now()

		categories := []string{"electronics", "books", "clothing", "food", "toys"}
		insertedCount := 0

		// We'll randomly select which product to use for testing within the actual range
		var queryVector []float32
		var randomProductID int

		// Timing variables
		var vectorGenTime, insertTime time.Duration

		// Begin transaction for better performance
		_, err = conn.GetConnection().Exec("BEGIN TRANSACTION", nil)

		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Build the batched INSERT statement
		placeholders := ""

		for i := range insertBatchSize {
			if i > 0 {
				placeholders += ", "
			}

			placeholders += "(?, ?, ?, ?, ?)"
		}

		insertSQL := fmt.Sprintf(`INSERT INTO products (id, name, category, price, embedding) VALUES %s`, placeholders)

		// Prepare the batched INSERT statement once
		insertStmt, err := conn.GetConnection().Prepare(ctx, insertSQL)

		if err != nil {
			t.Fatalf("Failed to prepare insert statement: %v", err)
		}

		for batch := 0; batch < totalProducts/generateBatchSize; batch++ {
			vecGenStart := time.Now()
			vectors := GenerateBatch(generateBatchSize, vectorDim)
			vectorGenTime += time.Since(vecGenStart)

			// On first batch, select a random product from the entire range
			if batch == 0 && randomProductID == 0 {
				randomProductID = rand.Int()%totalProducts + 1
			}

			// Pre-convert all vectors to binary blobs once
			vectorBlobs := make([][]byte, len(vectors))

			for idx, vec := range vectors {
				vectorBlobs[idx] = VectorToBlob(vec)
			}

			// Calculate the ID range for this batch
			batchStartID := batch*generateBatchSize + 1
			batchEndID := (batch + 1) * generateBatchSize

			// Capture the query vector if it's in this batch
			if queryVector == nil && randomProductID >= batchStartID && randomProductID <= batchEndID {
				vectorIndex := randomProductID - batchStartID // 0-based index within this batch
				if vectorIndex >= 0 && vectorIndex < len(vectors) {
					queryVector = vectors[vectorIndex]
					t.Logf("Captured query vector for product %d: batch=%d, idx=%d, first 3 vals=[%.6f, %.6f, %.6f]",
						randomProductID, batch, vectorIndex, queryVector[0], queryVector[1], queryVector[2])
				}
			}

			// Process vectors in insert batches
			for i := 0; i < len(vectors); i += insertBatchSize {
				end := i + insertBatchSize
				if end > len(vectors) {
					end = len(vectors)
				}

				batchVectors := vectors[i:end]
				params := make([]sqlite3.StatementParameter, 0, len(batchVectors)*5)

				for j := range batchVectors {
					productID := batch*generateBatchSize + i + j + 1

					// Log when we're inserting the random product
					if productID == randomProductID {
						vectorIdx := i + j
						t.Logf("Inserting random product %d: vectorIdx=%d, first 3 vals=[%.6f, %.6f, %.6f]",
							productID, vectorIdx, vectors[vectorIdx][0], vectors[vectorIdx][1], vectors[vectorIdx][2])
					}

					category := categories[productID%len(categories)]
					price := 10.0 + float64(productID%1000)

					params = append(params,
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(productID)},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(fmt.Sprintf("Product %d", productID))},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeText, Value: []byte(category)},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeFloat, Value: price},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: vectorBlobs[i+j]},
					)
				}

				// Execute the prepared statement with batched parameters
				insertStart := time.Now()
				err = insertStmt.Sqlite3Statement.Exec(sqlite3.NewResult(), params...)
				insertTime += time.Since(insertStart)

				if err != nil {
					_, rollbackErr := conn.GetConnection().Exec("ROLLBACK", nil)

					if rollbackErr != nil {
						t.Logf("Failed to rollback: %v", rollbackErr)
					}

					t.Fatalf("Failed to insert batch at product %d: %v", batch*generateBatchSize+i, err)
				}

				insertedCount += len(batchVectors)
			}

			if (batch+1)%10 == 0 {
				t.Logf("  Inserted %d products...", insertedCount)
			}
		}

		// Commit the transaction
		_, err = conn.GetConnection().Exec("COMMIT", nil)

		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		generateDuration := time.Since(startGenerate)

		t.Logf("✓ Generated and inserted %d products in %v", totalProducts, generateDuration)
		t.Logf("  - Vector dimensions: %d", vectorDim)
		t.Logf("  - Insertion rate: %.0f products/sec", float64(totalProducts)/generateDuration.Seconds())
		t.Logf("  - Time breakdown:")
		t.Logf("    • Vector generation: %v (%.1f%%)", vectorGenTime, 100*vectorGenTime.Seconds()/generateDuration.Seconds())
		t.Logf("    • INSERT execution: %v (%.1f%%)", insertTime, 100*insertTime.Seconds()/generateDuration.Seconds())

		t.Logf("Randomly selected product ID: %d", randomProductID)

		if queryVector == nil {
			t.Fatal("Query vector was not captured during insert")
		}

		// Verify the random product exists in database
		stmt, err := conn.GetConnection().Prepare(ctx, `SELECT rowid, embedding FROM products WHERE id = ?`)

		if err != nil {
			t.Fatalf("Failed to prepare verify query: %v", err)
		}

		verifyResult := sqlite3.NewResult()

		err = stmt.Sqlite3Statement.Exec(verifyResult,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(randomProductID)},
		)

		if err != nil {
			t.Fatalf("Failed to verify product exists: %v", err)
		}

		if len(verifyResult.Rows) == 0 {
			t.Fatalf("Product %d does not exist in database!", randomProductID)
		}

		storedRowID := int64(binary.LittleEndian.Uint64(verifyResult.Rows[0][0].ColumnValue))
		storedBlob := verifyResult.Rows[0][1].ColumnValue

		t.Logf("✓ Product %d exists: rowid=%d, blob_size=%d bytes", randomProductID, storedRowID, len(storedBlob))

		// Use the STORED blob from database for searching (not regenerated)
		queryVectorBlob := storedBlob

		// Test k-NN search with different metrics
		metrics := []string{"l2", "cosine", "dot"}

		// CRITICAL DEBUG: Check if rowid matches id
		if storedRowID != int64(randomProductID) {
			t.Logf("  ⚠️  WARNING: rowid (%d) != id (%d)", storedRowID, randomProductID)
		}

		k := 10

		for _, metric := range metrics {
			t.Logf("\nTesting k-NN search with %s metric (k=%d)...", metric, k)

			startQuery := time.Now()

			// Prepare statement with metric as literal (vector_scan requires it)
			stmt, err := conn.GetConnection().Prepare(ctx, fmt.Sprintf(`
				SELECT p.id, p.name, p.category, p.price, v.distance
				FROM products p
				JOIN vector_scan('products', 'embedding', ?, ?, '%s') v
				  ON p.rowid = v.rowid
				ORDER BY v.distance
				LIMIT ?
			`, metric))

			if err != nil {
				t.Fatalf("Failed to prepare %s query: %v", metric, err)
			}

			knnResult := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(knnResult,
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: queryVectorBlob},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(k)},
			)

			if err != nil {
				t.Fatalf("Failed to execute %s query: %v", metric, err)
			}

			queryDuration := time.Since(startQuery)

			if len(knnResult.Rows) != k {
				t.Errorf("%s: Expected %d results, got %d", metric, k, len(knnResult.Rows))
			}

			// Verify results are ordered by distance
			var lastDistance = math.Inf(-1)

			for i, row := range knnResult.Rows {
				if len(row) < 5 {
					t.Fatalf("Row %d has insufficient columns", i)
				}

				distance := math.Float64frombits(binary.LittleEndian.Uint64(row[4].ColumnValue))

				if i > 0 && distance < lastDistance {
					t.Errorf("%s: Results not ordered by distance at row %d: %.6f < %.6f",
						metric, i, distance, lastDistance)
				}

				lastDistance = distance
			}

			// Check if the query product appears anywhere in the results
			foundQueryProduct := false
			var queryProductPosition int
			allIDs := make([]int, 0, len(knnResult.Rows))

			t.Logf("  Checking %d results for product ID %d:", len(knnResult.Rows), randomProductID)

			for i, row := range knnResult.Rows {
				if len(row) < 5 {
					continue
				}

				id := int64(binary.LittleEndian.Uint64(row[0].ColumnValue))
				allIDs = append(allIDs, int(id))

				if int(id) == randomProductID {
					foundQueryProduct = true
					queryProductPosition = i + 1
				}
			}

			if !foundQueryProduct {
				t.Errorf("%s: Query product (id=%d) not found in k=%d results! IDs returned: %v",
					metric, randomProductID, k, allIDs)
			} else if queryProductPosition != 1 {
				t.Errorf("%s: Query product found at position %d (expected position 1)", metric, queryProductPosition)
			}

			// Verify the first result is the query product itself (distance ~0 for l2/cosine)
			if len(knnResult.Rows) > 0 {
				firstID := int64(binary.LittleEndian.Uint64(knnResult.Rows[0][0].ColumnValue))
				firstDistance := math.Float64frombits(binary.LittleEndian.Uint64(knnResult.Rows[0][4].ColumnValue))

				// For l2 and cosine, distance should be ~0 for exact match
				// For dot product, any value is valid (it's the actual dot product, not a distance metric)
				if int(firstID) == randomProductID && metric != "dot" {
					if firstDistance > 0.0001 || firstDistance < -0.0001 {
						t.Errorf("%s: Query product is first but distance is %.6f (expected ~0)", metric, firstDistance)
					}
				}
			}

			t.Logf("✓ %s metric completed in %v", metric, queryDuration)
			t.Logf("  - Query rate: %.2f queries/sec", 1.0/queryDuration.Seconds())
			t.Logf("  - Throughput: %.0f vectors/sec", float64(totalProducts)/queryDuration.Seconds())
		}
	})
}
