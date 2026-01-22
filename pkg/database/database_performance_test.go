package database_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func runBechmark(b *testing.B, db test.TestDatabase, app *server.App) {
	// Get database connection
	conn, err := app.DatabaseManager.ConnectionManager().Get(
		db.DatabaseKey.DatabaseID,
		db.DatabaseKey.DatabaseBranchID,
	)

	defer app.DatabaseManager.ConnectionManager().Release(conn)

	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	result := &sqlite3.Result{}

	statement, err := conn.GetConnection().Prepare(
		b.Context(),
		`CREATE TABLE IF NOT EXISTS benchmark_test (
				id INTEGER PRIMARY KEY,
				name TEXT,
				value INTEGER,
				created_at INTEGER
			)`,
	)

	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}

	// Create test table
	err = conn.GetConnection().Query(result, statement.Sqlite3Statement, nil)

	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.Run("Insert", func(b *testing.B) {
		b.ResetTimer()

		statement, err := conn.GetConnection().Prepare(
			b.Context(),
			"INSERT INTO benchmark_test (name, value, created_at) VALUES (?, ?, ?)",
		)

		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		// string allocation optimization
		var sb strings.Builder
		sb.Grow(32)
		now := time.Now().Unix()

		parameters := []sqlite3.StatementParameter{
			{
				Value: sb.String(),
			},
			{
				Value: 0,
			},
			{
				Value: now,
			},
		}

		for i := 0; b.Loop(); i++ {
			sb.Reset()
			sb.WriteString("test_")
			sb.WriteString(fmt.Sprintf("%d", i))

			parameters[0].Value = sb.String()
			parameters[1].Value = i

			err = conn.GetConnection().Query(
				result,
				statement.Sqlite3Statement,
				parameters,
			)

			if err != nil {
				b.Fatalf("Failed to insert: %v", err)
			}
		}

		b.StopTimer()
	})

	b.Run("Select", func(b *testing.B) {
		// Insert some test data first
		statement, err := conn.GetConnection().Prepare(
			b.Context(),
			"INSERT INTO benchmark_test (name, value, created_at) VALUES (?, ?, ?)",
		)

		if err != nil {
			b.Fatalf("Failed to prepare insert statement: %v", err)
		}

		sb := strings.Builder{}
		sb.Grow(32)

		parameters := []sqlite3.StatementParameter{
			{
				Value: "",
			},
			{
				Value: 0,
			},
			{
				Value: 0,
			},
		}

		now := time.Now().Unix()

		for i := range 1000 {
			sb.Reset()
			sb.WriteString("test_")
			sb.WriteString(fmt.Sprintf("%d", i))

			parameters[0].Value = sb.String()
			parameters[1].Value = i
			parameters[2].Value = now

			err = conn.GetConnection().Query(
				result,
				statement.Sqlite3Statement,
				parameters,
			)

			if err != nil {
				b.Fatalf("Failed to insert test data: %v", err)
			}
		}

		b.ResetTimer()

		statement, err = conn.GetConnection().Prepare(b.Context(), "SELECT id, name, value FROM benchmark_test LIMIT 100")

		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		for b.Loop() {
			err := conn.GetConnection().Query(result, statement.Sqlite3Statement, nil)

			if err != nil {
				b.Fatalf("Failed to select: %v", err)
			}

			count := 0

			for range result.Rows {
				// var id, value int
				// var name string

				// if err := row.Scan(&id, &name, &value); err != nil {
				// 	b.Fatalf("Failed to scan: %v", err)
				// }

				count++
			}

			// err = result.Close()

			// if err != nil {
			// 	b.Fatalf("Failed to close result: %v", err)
			// }

			if count != 100 {
				b.Fatalf("Expected 100 rows, got %d", count)
			}
		}

		b.StopTimer()
	})

	b.Run("Update", func(b *testing.B) {
		statement, err := conn.GetConnection().Prepare(
			b.Context(),
			"UPDATE benchmark_test SET value = ? WHERE id = ?",
		)

		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		// Insert some test data first
		for i := range 1000 {
			err = conn.GetConnection().Query(
				result,
				statement.Sqlite3Statement,
				[]sqlite3.StatementParameter{
					{
						Value: fmt.Sprintf("test_%d", i),
					},
					{
						Value: i,
					},
				},
			)

			if err != nil {
				b.Fatalf("Failed to insert test data: %v", err)
			}
		}

		b.ResetTimer()

		parameters := []sqlite3.StatementParameter{
			{
				Value: 0,
			},
			{
				Value: 0,
			},
		}

		for i := 0; b.Loop(); i++ {
			parameters[0].Value = i * 2
			parameters[1].Value = (i % 1000) + 1

			err = conn.GetConnection().Query(
				result,
				statement.Sqlite3Statement,
				parameters,
			)

			if err != nil {
				b.Fatalf("Failed to update: %v", err)
			}
		}

		b.StopTimer()
	})

	b.Run("Transaction", func(b *testing.B) {
		// Pre-allocate to avoid allocations in hot loop
		params := make([]sqlite3.StatementParameter, 3)
		nameBuf := make([]byte, 0, 64)

		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			err := conn.GetConnection().Begin()

			if err != nil {
				b.Fatalf("Failed to begin transaction: %v", err)
			}

			for j := range 10 {
				// Reuse buffer for name formatting
				nameBuf = nameBuf[:0]
				nameBuf = fmt.Appendf(nameBuf, "test_%d_%d", i, j)

				params[0].Value = string(nameBuf)
				params[1].Value = j
				params[2].Value = time.Now().Unix()

				result, err := conn.GetConnection().Exec(
					"INSERT INTO benchmark_test (name, value, created_at) VALUES (?, ?, ?)",
					params,
				)

				if err != nil {
					b.Fatalf("Failed to insert in transaction: %v", err)
				}

				conn.GetConnection().ResultPool().Put(result)
			}

			if err = conn.GetConnection().Commit(); err != nil {
				b.Fatalf("Failed to commit transaction: %v", err)
			}
		}

		b.StopTimer()
	})
}

// BenchmarkDatabaseQueries benchmarks query performance
func BenchmarkDatabaseQueries(b *testing.B) {
	test.RunWithApp(b, func(app *server.App) {
		runBechmark(b, test.MockDatabase(app), app)
	})
}

func BenchmarkEncryptedDatabaseQueries(b *testing.B) {
	test.Run(b, func() {
		server := test.NewTestServerWithEncryption(b)
		defer server.Shutdown()

		app := server.App
		db := test.MockEncryptedDatabase(app)

		runBechmark(b, db, app)
	})
}
