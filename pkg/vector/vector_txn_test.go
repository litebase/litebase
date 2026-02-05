package vector_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// makeZeroVectorBlob builds a minimal valid vector blob with given dimension
func makeZeroVectorBlob(dim int) []byte {
	// version(1) + type(1) + dimensions(4) + data(dim*4)
	size := 1 + 1 + 4 + dim*4
	b := make([]byte, size)
	b[0] = 0x01
	b[1] = 0x01

	binary.LittleEndian.PutUint32(b[2:6], uint32(dim))

	// zero-filled data
	for i := range dim {
		binary.LittleEndian.PutUint32(b[6+i*4:6+(i+1)*4], math.Float32bits(0))
	}

	return b
}

// TestTxnInsertSearch verifies inserting into a vector index inside a DB
// transaction using a connection from the connection manager and attempts
// to run a vector search from within that transaction.
func TestTxnInsertSearch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create a simple vector index
		_, err = dbConn.Exec(`
            CREATE VIRTUAL TABLE embeddings USING vector_index(
				embedding_id INTEGER,
                vector BLOB,
                dimensions=4,
                distance_metric=0
            )
        `, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Use Transaction wrapper which sets timestamps correctly
		err = dbConn.Transaction(false, func(txConn *database.DatabaseConnection) error {
			// Insert one vector inside the transaction
			_, err := txConn.Exec("INSERT INTO embeddings(embedding_id, vector) VALUES (?, ?)", []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: int64(1)},
				{Type: sqlite3.ParameterTypeBlob, Value: makeZeroVectorBlob(4)},
			})

			if err != nil {
				t.Fatalf("Failed to insert inside transaction: %v", err)
			}

			// Query the virtual table directly to read back user-defined columns
			// This queries the _vectors shadow table through the virtual table interface
			result, err := txConn.Exec("SELECT id, embedding_id FROM embeddings WHERE id = 1", nil)

			if err != nil {
				t.Fatalf("Failed to query embeddings table: %v", err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected 1 row from embeddings query, got 0")
			}

			if len(result.Rows[0]) < 2 {
				t.Fatalf("Expected at least 2 columns, got %d", len(result.Rows[0]))
			}

			// Check column types and values
			col0 := result.Rows[0][0]
			col1 := result.Rows[0][1]

			// The virtual table may return different column types - handle both cases
			var rowID, embeddingID int64

			if col0.ColumnType == sqlite3.ColumnTypeInteger && len(col0.ColumnValue) >= 8 {
				rowID = col0.Int64()
			} else {
				t.Fatalf("Unexpected column 0 type or length: type=%v, len=%d", col0.ColumnType, len(col0.ColumnValue))
			}

			if col1.ColumnType == sqlite3.ColumnTypeInteger && len(col1.ColumnValue) >= 8 {
				embeddingID = col1.Int64()
			} else {
				t.Fatalf("Unexpected column 1 type or length: type=%v, len=%d", col1.ColumnType, len(col1.ColumnValue))
			}

			if rowID != 1 {
				t.Errorf("Expected rowid=1, got %d", rowID)
			}

			if embeddingID != 1 {
				t.Errorf("Expected embedding_id=1, got %d", embeddingID)
			}

			t.Logf("✓ Successfully queried user-defined column: id=%d, embedding_id=%d", rowID, embeddingID)

			queryVector := makeZeroVectorBlob(4)

			t.Log("Attempting vector_search within transaction (expecting an error)...")
			_, err = txConn.Exec(
				"SELECT rowid, distance FROM vector_search('embeddings', 'vector', ?, 1)",
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeBlob, Value: queryVector},
				},
			)

			if err == nil {
				t.Fatalf("Expected vector_search to return an error when executed inside a transaction, but it succeeded")
			}

			t.Logf("✓ vector_search returned expected error inside transaction: %v", err)

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}

	})
}
