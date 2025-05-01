package tests

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/sqlite3"
)

func TestDatabaseServerCorrectness_100(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		result := db.GetConnection().ResultPool().Get()
		defer db.GetConnection().ResultPool().Put(result)

		// Create a table for testing
		_, err := db.GetConnection().Exec(
			"CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT, updated_at TEXT)",
			nil,
		)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		statement, _ := db.GetConnection().Prepare(
			db.GetConnection().Context(),
			[]byte("INSERT INTO test (name, created_at, updated_at) VALUES (?, ?, ?)"),
		)

		count := 0

		for range 10 {
			for range 100 {
				count++

				err := db.GetConnection().Query(
					result,
					statement.Sqlite3Statement,
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("name"),
						},
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("2023-10-01 00:00:00"),
						},
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("2023-10-01 00:00:00"),
						},
					},
				)

				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}
			}

			// verify the count
			result, err := db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			if result.RowCount() != 1 {
				t.Fatalf("Expected 1 row, got %d", result.RowCount())
			}

			row := result.Row(0)

			if row[0].ColumnType != sqlite3.ColumnTypeInteger {
				t.Fatalf("Expected column type INTEGER, got %v", row[0].ColumnType)
			}

			if row[0].ColumnValue == nil {
				t.Fatalf("Expected column value to be not nil")
			}

			if row[0].Int64() != int64(count) {
				t.Fatalf("Expected column value to be %d, got %d", count, row[0].Int64())
			}
		}
	})
}

func TestDatabaseServerCorrectness_1000(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		result := db.GetConnection().ResultPool().Get()
		defer db.GetConnection().ResultPool().Put(result)

		// Create a table for testing
		_, err := db.GetConnection().Exec(
			"CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT, updated_at TEXT)",
			nil,
		)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		statement, _ := db.GetConnection().Prepare(
			db.GetConnection().Context(),
			[]byte("INSERT INTO test (name, created_at, updated_at) VALUES (?, ?, ?)"),
		)

		count := 0

		for range 10 {
			for range 1000 {
				count++

				err := db.GetConnection().Query(
					result,
					statement.Sqlite3Statement,
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("name"),
						},
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("2023-10-01 00:00:00"),
						},
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("2023-10-01 00:00:00"),
						},
					},
				)

				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}
			}

			// verify the count
			result, err := db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			if result.RowCount() != 1 {
				t.Fatalf("Expected 1 row, got %d", result.RowCount())
			}

			row := result.Row(0)

			if row[0].ColumnType != sqlite3.ColumnTypeInteger {
				t.Fatalf("Expected column type INTEGER, got %v", row[0].ColumnType)
			}

			if row[0].ColumnValue == nil {
				t.Fatalf("Expected column value to be not nil")
			}

			if row[0].Int64() != int64(count) {
				t.Fatalf("Expected column value to be %d, got %d", count, row[0].Int64())
			}
		}
	})
}
