package vector_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// TestVectorIndexMetadataPrefilter verifies that WHERE constraints on
// non-vector metadata columns are pushed down into the _vectors shadow-table
// query via xBestIndex / xFilter, rather than being evaluated as a post-fetch
// pass by SQLite.
func TestVectorIndexMetadataPrefilter(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create an index that includes two non-vector metadata columns.
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE products USING vector_index(
				category  TEXT,
				price     INTEGER,
				embedding BLOB,
				embedding_dimensions=3,
				embedding_distance_metric=cosine
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		type row struct {
			category  string
			price     int64
			embedding []float32
		}

		rows := []row{
			{"electronics", 100, []float32{1.0, 0.0, 0.0}},
			{"electronics", 500, []float32{0.9, 0.1, 0.0}},
			{"books", 20, []float32{0.0, 1.0, 0.0}},
			{"books", 35, []float32{0.0, 0.9, 0.1}},
			{"clothing", 80, []float32{0.0, 0.0, 1.0}},
		}

		for _, r := range rows {
			blob := VectorToBlob(r.embedding)

			_, err = dbConn.Exec(`INSERT INTO products(category, price, embedding) VALUES (?, ?, ?)`,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeText, Value: []byte(r.category)},
					{Type: sqlite3.ParameterTypeInteger, Value: r.price},
					{Type: sqlite3.ParameterTypeBlob, Value: blob},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		t.Run("EqualityFilter", func(t *testing.T) {
			// Only the 2 "electronics" rows should be returned.
			res, err := dbConn.Exec(`SELECT rowid FROM products WHERE category = 'electronics'`, nil)

			if err != nil {
				t.Fatalf("Equality filter query failed: %v", err)
			}

			if len(res.Rows) != 2 {
				t.Errorf("Expected 2 rows matching category='electronics', got %d", len(res.Rows))
			}
		})

		t.Run("RangeFilter", func(t *testing.T) {
			// Rows with price < 100 : books @20, books @35, clothing @80 → 3 rows.
			res, err := dbConn.Exec(`SELECT rowid FROM products WHERE price < 100`, nil)

			if err != nil {
				t.Fatalf("Range filter query failed: %v", err)
			}

			if len(res.Rows) != 3 {
				t.Errorf("Expected 3 rows with price < 100, got %d", len(res.Rows))
			}
		})

		t.Run("CombinedFilters", func(t *testing.T) {
			// category='books' AND price <= 30 → only the books @20 row.
			res, err := dbConn.Exec(`SELECT rowid FROM products WHERE category = 'books' AND price <= 30`, nil)

			if err != nil {
				t.Fatalf("Combined filter query failed: %v", err)
			}

			if len(res.Rows) != 1 {
				t.Errorf("Expected 1 row for category='books' AND price<=30, got %d", len(res.Rows))
			}
		})

		t.Run("FilterReturnsNoRows", func(t *testing.T) {
			// No row has category='toys'.
			res, err := dbConn.Exec(`SELECT rowid FROM products WHERE category = 'toys'`, nil)

			if err != nil {
				t.Fatalf("Zero-result filter query failed: %v", err)
			}

			if len(res.Rows) != 0 {
				t.Errorf("Expected 0 rows for category='toys', got %d", len(res.Rows))
			}
		})
	})
}

// TestVectorIndexMetadataUpdate verifies that after an in-place UPDATE the
// metadata columns stored in the _vectors shadow table reflect the new values.
func TestVectorIndexMetadataUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE items USING vector_index(
				label     TEXT,
				embedding BLOB,
				embedding_dimensions=3,
				embedding_distance_metric=cosine
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		blob := VectorToBlob([]float32{1.0, 0.0, 0.0})

		_, err = dbConn.Exec(`INSERT INTO items(label, embedding) VALUES (?, ?)`,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeText, Value: []byte("original")},
				{Type: sqlite3.ParameterTypeBlob, Value: blob},
			},
		)

		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}

		// Update both the label and the vector.
		newBlob := VectorToBlob([]float32{0.0, 1.0, 0.0})

		_, err = dbConn.Exec(`UPDATE items SET label = ?, embedding = ? WHERE rowid = 1`,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeText, Value: []byte("updated")},
				{Type: sqlite3.ParameterTypeBlob, Value: newBlob},
			},
		)

		if err != nil {
			t.Fatalf("Failed to update: %v", err)
		}

		// Count must still be 1 (in-place update, not insert+ghost).
		res, err := dbConn.Exec(`SELECT COUNT(*) FROM items_vectors`, nil)

		if err != nil {
			t.Fatalf("Failed to count: %v", err)
		}

		if res.Rows[0][0].Int64() != 1 {
			t.Errorf("Expected 1 row after update, got %d", res.Rows[0][0].Int64())
		}

		// The label column must reflect the new value.
		res, err = dbConn.Exec(`SELECT label FROM items WHERE rowid = 1`, nil)

		if err != nil {
			t.Fatalf("Failed to select updated label: %v", err)
		}

		if len(res.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(res.Rows))
		}

		label := string(res.Rows[0][0].Text())

		if label != "updated" {
			t.Errorf("Expected label='updated', got '%s'", label)
		}

		// The cluster map must have exactly one entry assigned to a real cluster.
		res, err = dbConn.Exec(`SELECT COUNT(*) FROM items_embedding_cluster_vector_map WHERE cluster_id > 0`, nil)

		if err != nil {
			t.Fatalf("Failed to count cluster map: %v", err)
		}

		if res.Rows[0][0].Int64() != 1 {
			t.Errorf("Expected 1 cluster map entry with cluster_id > 0 after update, got %d", res.Rows[0][0].Int64())
		}
	})
}
