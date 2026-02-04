package vector_test

import (
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorIndexValidation(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		t.Run("DisallowUserDefinedIdColumn", func(t *testing.T) {
			// Attempt to create a vector index with an 'id' column should fail
			_, err := dbConn.Exec(`
				CREATE VIRTUAL TABLE test_id_column USING vector_index(
					id INTEGER,
					vector BLOB,
					dimensions=128
				)
			`, nil)

			if err == nil {
				t.Fatal("Expected error when creating table with 'id' column, but got none")
			}

			expectedError := "Cannot define 'id' column - it is automatically generated as the rowid"

			if !strings.Contains(err.Error(), expectedError) {
				t.Errorf("Expected error message to contain %q, but got: %v", expectedError, err)
			}

			t.Logf("✓ Correctly rejected user-defined 'id' column: %v", err)
		})

		t.Run("AllowValidTableCreation", func(t *testing.T) {
			// Normal table creation without 'id' column should work
			_, err := dbConn.Exec(`
				CREATE VIRTUAL TABLE test_valid USING vector_index(
					vector BLOB,
					dimensions=128
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create valid vector index: %v", err)
			}

			t.Log("✓ Successfully created table without user-defined 'id' column")

			// Verify the table has auto-generated id/rowid
			queryVec := NewTestVector(128)
			queryBlob := VectorToBlob(queryVec)

			_, err = dbConn.Exec("INSERT INTO test_valid(vector) VALUES (?)",
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeBlob, Value: queryBlob},
				})

			if err != nil {
				t.Fatalf("Failed to insert into table: %v", err)
			}

			// Verify we can query the rowid
			res, err := dbConn.Exec("SELECT rowid FROM test_valid LIMIT 1", nil)

			if err != nil {
				t.Fatalf("Failed to query rowid: %v", err)
			}

			if len(res.Rows) == 0 {
				t.Error("Expected to find inserted row")
			} else {
				rowid := res.Rows[0][0].Int64()
				t.Logf("✓ Auto-generated rowid works correctly: %d", rowid)
			}
		})
	})
}
