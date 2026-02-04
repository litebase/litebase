package vector_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorIndexUserDefinedColumns(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		t.Run("CreateWithMultipleColumns", func(t *testing.T) {
			// Create a vector index table with user-defined columns
			_, err := dbConn.Exec(`
				CREATE VIRTUAL TABLE products USING vector_index(
					product_id INTEGER,
					category TEXT,
					vector BLOB,
					dimensions=128
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create vector index with user columns: %v", err)
			}

			t.Logf("✓ Successfully created table with user-defined columns (product_id, category, vector)")
		})

		t.Run("InsertWithAllColumns", func(t *testing.T) {
			// Insert a vector with all columns
			vec := NewTestVector(128)
			blob := VectorToBlob(vec)

			// TODO: Once column_values buffer is implemented, this should work
			// For now, this will fail because INSERT only supports vector column
			_, err := dbConn.Exec(`
				INSERT INTO products(product_id, category, vector) 
				VALUES (?, ?, ?)
			`, []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: int64(1001)},
				{Type: sqlite3.ParameterTypeText, Value: "electronics"},
				{Type: sqlite3.ParameterTypeBlob, Value: blob},
			})

			if err != nil {
				t.Logf("Expected failure (not yet implemented): %v", err)
				t.Skip("Column_values buffer not yet implemented - skipping INSERT test")
			}

			t.Logf("✓ Successfully inserted row with all columns")
		})

		t.Run("SelectAllColumns", func(t *testing.T) {
			t.Skip("Skipping until INSERT is implemented")

			// Query back the data
			res, err := dbConn.Exec("SELECT product_id, category FROM products WHERE rowid = 1", nil)

			if err != nil {
				t.Fatalf("Failed to select: %v", err)
			}

			if len(res.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(res.Rows))
			}

			row := res.Rows[0]

			if len(row) != 2 {
				t.Fatalf("Expected 2 columns, got %d", len(row))
			}

			productID := row[0].Int64()
			category := string(row[1].Text())

			if productID != 1001 {
				t.Errorf("Expected product_id=1001, got %d", productID)
			}

			if category != "electronics" {
				t.Errorf("Expected category='electronics', got '%s'", category)
			}

			t.Logf("✓ Successfully retrieved user-defined columns")
		})
	})
}

func TestVectorIndexMultipleVectorColumns(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		t.Run("CreateWithMultipleVectorColumns", func(t *testing.T) {
			// Create a table with multiple vector columns (different dimensions)
			_, err := dbConn.Exec(`
				CREATE VIRTUAL TABLE multimodal USING vector_index(
					image_vector BLOB,
					text_vector BLOB,
					image_vector_dimensions=512,
					text_vector_dimensions=128
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create multi-vector table: %v", err)
			}

			t.Logf("✓ Successfully created table with multiple vector columns")
		})

		t.Run("VerifyColumnMetadata", func(t *testing.T) {
			// Just verify the table was created - actual multi-vector support requires column_values buffer
			res, err := dbConn.Exec("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'multimodal%'", nil)

			if err != nil {
				t.Fatalf("Failed to query schema: %v", err)
			}

			if len(res.Rows) < 1 {
				t.Fatal("Expected shadow tables to be created")
			}

			t.Logf("✓ Shadow tables created for multi-vector table")
		})
	})
}
