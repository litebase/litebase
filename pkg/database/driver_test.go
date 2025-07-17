package database_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

// TestLitebaseDriver tests the database/sql driver implementation
func TestLitebaseDriver(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// The driver should already be registered by the DatabaseManager
		// Open connection to the system database
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			t.Fatalf("failed to open system database: %v", err)
		}

		defer db.Close()

		// Test ping with a timeout context to avoid hanging
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = db.PingContext(ctx)

		if err != nil {
			t.Fatalf("failed to ping database: %v", err)
		}

		// Test table creation in system database
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS test_users (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				email TEXT UNIQUE NOT NULL,
				created_at TEXT
			)
		`)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Test insert
		result, err := db.Exec("INSERT INTO test_users (name, email) VALUES (?, ?)", "John Doe", "john@test.com")

		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}

		// Test LastInsertId
		lastID, err := result.LastInsertId()

		if err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		}
		if lastID <= 0 {
			t.Fatalf("expected last insert id to be greater than 0, got %d", lastID)
		}

		// Test RowsAffected
		affected, err := result.RowsAffected()

		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}

		if affected != 1 {
			t.Fatalf("expected 1 row affected, got %d", affected)
		}

		// Test QueryRow
		var user struct {
			ID    int64
			Name  string
			Email string
		}

		err = db.QueryRow(
			"SELECT id, name, email FROM test_users WHERE id = ?",
			lastID,
		).Scan(&user.ID, &user.Name, &user.Email)

		if err != nil {
			t.Fatalf("failed to query user: %v", err)
		}

		if user.ID != lastID || user.Name != "John Doe" || user.Email != "john@test.com" {
			t.Fatalf("unexpected user data: %+v", user)
		}

		// Test Query with multiple rows
		_, err = db.Exec("INSERT INTO test_users (name, email) VALUES (?, ?)", "Jane Doe", "jane@test.com")

		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}

		rows, err := db.Query("SELECT id, name, email FROM test_users ORDER BY id")

		if err != nil {
			t.Fatalf("failed to query users: %v", err)
		}

		defer rows.Close()

		var users []struct {
			ID    int64
			Name  string
			Email string
		}

		for rows.Next() {
			var u struct {
				ID    int64
				Name  string
				Email string
			}

			err := rows.Scan(&u.ID, &u.Name, &u.Email)

			if err != nil {
				t.Fatalf("failed to scan user: %v", err)
			}

			users = append(users, u)
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("failed to iterate over rows: %v", err)
		}

		if len(users) != 2 {
			t.Fatalf("expected 2 users, got %d", len(users))
		}

		if users[0].Name != "John Doe" || users[1].Name != "Jane Doe" {
			t.Fatalf("unexpected user names: %+v", users)
		}

		// Test prepared statements
		stmt, err := db.Prepare("SELECT name FROM test_users WHERE id = ?")

		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}

		defer stmt.Close()

		var name string
		err = stmt.QueryRow(lastID).Scan(&name)
		if err != nil {
			t.Fatalf("failed to execute prepared statement: %v", err)
		}

		if name != "John Doe" {
			t.Fatalf("unexpected user name: %s", name)
		}

		// Test transactions
		tx, err := db.Begin()

		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("INSERT INTO test_users (name, email) VALUES (?, ?)", "Test User", "test@test.com")

		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}

		err = tx.Rollback()

		if err != nil {
			t.Fatalf("failed to rollback transaction: %v", err)
		}

		// Verify rollback worked
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_users").Scan(&count)

		if err != nil {
			t.Fatalf("failed to query user count: %v", err)
		}

		if count != 2 {
			t.Fatalf("expected 2 users, got %d", count)
		}

		// Test successful transaction
		tx, err = db.Begin()

		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("UPDATE test_users SET name = ? WHERE id = ?", "John Updated", lastID)

		if err != nil {
			t.Fatalf("failed to update user: %v", err)
		}

		err = tx.Commit()

		if err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		// Verify commit worked
		err = db.QueryRow("SELECT name FROM test_users WHERE id = ?", lastID).Scan(&name)

		if err != nil {
			t.Fatalf("failed to query user: %v", err)
		}

		if name != "John Updated" {
			t.Fatalf("unexpected user name: %s", name)
		}

		// Test context operations
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_users").Scan(&count)

		if err != nil {
			t.Fatalf("failed to query user count: %v", err)
		}

		if count != 2 {
			t.Fatalf("expected 2 users, got %d", count)
		}

		// Clean up
		_, err = db.Exec("DROP TABLE test_users")

		if err != nil {
			t.Fatalf("failed to drop table: %v", err)
		}
	})
}

// TestLitebaseDriverSimple tests the basic driver functionality
func TestLitebaseDriverSimple(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Open connection to the system database
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			t.Fatalf("failed to open system database: %v", err)
		}

		defer db.Close()

		// Test ping with a timeout context to avoid hanging
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = db.PingContext(ctx)

		if err != nil {
			t.Fatalf("failed to ping database: %v", err)
		}

		// Test simple table creation
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_simple (id INTEGER PRIMARY KEY, name TEXT)`)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Test simple insert
		result, err := db.Exec("INSERT INTO test_simple (name) VALUES (?)", "test")

		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Test LastInsertId
		lastID, err := result.LastInsertId()

		if err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		}

		if lastID <= 0 {
			t.Fatalf("expected last insert id > 0, got %d", lastID)
		}

		// Test simple query
		var name string
		err = db.QueryRow("SELECT name FROM test_simple WHERE id = ?", lastID).Scan(&name)

		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if name != "test" {
			t.Fatalf("expected name 'test', got '%s'", name)
		}

		// Clean up
		_, err = db.Exec("DROP TABLE test_simple")

		if err != nil {
			t.Fatalf("failed to drop table: %v", err)
		}

		t.Log("Simple test passed!")
	})
}

// TestLitebaseDriverDataTypes tests various data type conversions
func TestLitebaseDriverDataTypes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Open connection to the system database
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}

		defer db.Close()

		// Create table with various data types
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS test_types (
				id INTEGER PRIMARY KEY,
				int_val INTEGER,
				float_val REAL,
				text_val TEXT,
				blob_val BLOB,
				bool_val INTEGER,
				time_val TEXT
			)
		`)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Test data
		testTime := time.Now().UTC()
		testBlob := []byte("binary data")

		// Insert test data
		_, err = db.Exec(`
		INSERT INTO test_types (int_val, float_val, text_val, blob_val, bool_val, time_val)
		VALUES (?, ?, ?, ?, ?, ?)
	`, int64(42), float64(3.14), "hello world", testBlob, true, testTime)

		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}

		// Query and verify data using sql.NullXXX types to see the raw values
		var (
			intValRaw   sql.NullInt64
			floatValRaw sql.NullFloat64
			textValRaw  sql.NullString
			blobValRaw  []byte
			boolValRaw  sql.NullInt64
			timeValRaw  sql.NullString
		)

		err = db.QueryRow(`
		SELECT int_val, float_val, text_val, blob_val, bool_val, time_val
		FROM test_types WHERE id = 1
	`).Scan(&intValRaw, &floatValRaw, &textValRaw, &blobValRaw, &boolValRaw, &timeValRaw)

		if err != nil {
			t.Fatalf("failed to query test data with raw types: %v", err)
		}

		// Query and verify data
		var (
			intVal     int64
			floatVal   float64
			textVal    string
			blobVal    []byte
			boolVal    bool
			timeValStr string // Scan as string first, then convert
		)

		err = db.QueryRow(`
		SELECT int_val, float_val, text_val, blob_val, bool_val, time_val
		FROM test_types WHERE id = 1
	`).Scan(&intVal, &floatVal, &textVal, &blobVal, &boolVal, &timeValStr)

		if err != nil {
			t.Fatalf("failed to query test data: %v", err)
		}

		// Convert time string to time.Time
		timeVal, err := time.Parse(time.RFC3339, timeValStr)
		if err != nil {
			t.Fatalf("failed to parse time: %v", err)
		}

		if intVal != int64(42) {
			t.Fatalf("unexpected int_val: %d", intVal)
		}

		if floatVal != float64(3.14) {
			t.Fatalf("unexpected float_val: %f", floatVal)
		}

		if textVal != "hello world" {
			t.Fatalf("unexpected text_val: %s", textVal)
		}

		if !bytes.Equal(blobVal, testBlob) {
			t.Fatalf("unexpected blob_val: %v", blobVal)
		}

		if boolVal != true {
			t.Fatalf("unexpected bool_val: %v", boolVal)
		}

		// Time comparison with tolerance due to RFC3339 string formatting
		// Times should be within 1 second of each other
		timeDiff := timeVal.Sub(testTime)
		if timeDiff < -time.Second || timeDiff > time.Second {
			t.Fatalf("time values too different: expected %v, got %v (diff: %v)", testTime, timeVal, timeDiff)
		}

		// Test NULL values
		_, err = db.Exec(`
		INSERT INTO test_types (id, int_val) VALUES (2, NULL)
	`)

		if err != nil {
			t.Fatalf("failed to insert NULL value: %v", err)
		}

		var nullableInt sql.NullInt64
		err = db.QueryRow("SELECT int_val FROM test_types WHERE id = 2").Scan(&nullableInt)

		if err != nil {
			t.Fatalf("failed to query NULL value: %v", err)
		}

		if nullableInt.Valid {
			t.Fatalf("expected NULL value, got %d", nullableInt.Int64)
		}

		// Clean up
		_, err = db.Exec("DROP TABLE test_types")
		if err != nil {
			t.Fatalf("failed to drop table: %v", err)
		}
	})
}

// BenchmarkLitebaseDriver benchmarks the driver performance
// func BenchmarkLitebaseDriver(b *testing.B) {
// 	test.RunWithApp(b, func(app *server.App) {
// 		// Open connection to the system database
// 		db, err := sql.Open("litebase-internal", "system/system")
// 		if err != nil {
// 			b.Fatalf("failed to open database: %v", err)
// 		}
// 		defer db.Close()

// 		// Setup
// 		_, err = db.Exec(`
// 			CREATE TABLE IF NOT EXISTS bench_test (
// 				id INTEGER PRIMARY KEY AUTOINCREMENT,
// 				data TEXT
// 			)
// 		`)
// 		if err != nil {
// 			b.Fatalf("failed to create table: %v", err)
// 		}

// 		b.ResetTimer()

// 		b.Run("Insert", func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				_, err := db.Exec("INSERT INTO bench_test (data) VALUES (?)", "test data")
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}
// 		})

// 		b.Run("Select", func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				var data string
// 				err := db.QueryRow("SELECT data FROM bench_test WHERE id = 1").Scan(&data)

// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}
// 		})

// 		b.Run("PreparedInsert", func(b *testing.B) {
// 			stmt, err := db.Prepare("INSERT INTO bench_test (data) VALUES (?)")

// 			if err != nil {
// 				b.Fatal(err)
// 			}

// 			defer stmt.Close()

// 			b.ResetTimer()

// 			for i := 0; i < b.N; i++ {
// 				_, err := stmt.Exec("prepared data")
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}
// 		})
// 	})
// }
