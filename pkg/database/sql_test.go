package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// Example struct to demonstrate struct scanning
type User struct {
	ID        int64     `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Email     string    `json:"email" db:"email"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

func TestSQLDriver(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Use the system database for testing the SQL driver
		systemDatabase := app.DatabaseManager.SystemDatabase()
		connection := systemDatabase.GetConnection()

		// Create SQL driver
		sqlDriver := database.NewSQLDriver(connection)

		t.Run("Create Table and Insert Data", func(t *testing.T) {
			// Create table
			_, err := sqlDriver.Exec(`
				CREATE TABLE test_users (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					name TEXT NOT NULL,
					email TEXT UNIQUE NOT NULL,
					created_at TEXT NOT NULL
				)
			`)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Insert test data
			now := time.Now().UTC()
			_, err = sqlDriver.Exec(
				"INSERT INTO test_users (name, email, created_at) VALUES (?, ?, ?)",
				"John Doe",
				"john@example.com",
				now.Format(time.RFC3339),
			)
			if err != nil {
				t.Fatalf("Failed to insert user: %v", err)
			}

			_, err = sqlDriver.Exec(
				"INSERT INTO test_users (name, email, created_at) VALUES (?, ?, ?)",
				"Jane Smith",
				"jane@example.com",
				now.Add(time.Hour).Format(time.RFC3339),
			)
			if err != nil {
				t.Fatalf("Failed to insert user: %v", err)
			}
		})

		t.Run("Query Single Row with Scan", func(t *testing.T) {
			var id int64
			var name, email string
			var createdAt time.Time

			err := sqlDriver.QueryRow(
				"SELECT id, name, email, created_at FROM test_users WHERE email = ?",
				"john@example.com",
			).Scan(&id, &name, &email, &createdAt)

			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if name != "John Doe" {
				t.Errorf("Expected name 'John Doe', got '%s'", name)
			}
			if email != "john@example.com" {
				t.Errorf("Expected email 'john@example.com', got '%s'", email)
			}
			if id == 0 {
				t.Error("Expected non-zero ID")
			}
		})

		t.Run("Query Single Row into Struct", func(t *testing.T) {
			var user User
			err := sqlDriver.ScanStruct(
				"SELECT id, name, email, created_at FROM test_users WHERE email = ?",
				&user,
				"jane@example.com",
			)

			if err != nil {
				t.Fatalf("Failed to scan into struct: %v", err)
			}

			if user.Name != "Jane Smith" {
				t.Errorf("Expected name 'Jane Smith', got '%s'", user.Name)
			}
			if user.Email != "jane@example.com" {
				t.Errorf("Expected email 'jane@example.com', got '%s'", user.Email)
			}
			if user.ID == 0 {
				t.Error("Expected non-zero ID")
			}
		})

		t.Run("Query Multiple Rows into Struct Slice", func(t *testing.T) {
			var users []User
			err := sqlDriver.ScanStructs(
				"SELECT id, name, email, created_at FROM test_users ORDER BY id",
				&users,
			)

			if err != nil {
				t.Fatalf("Failed to scan into struct slice: %v", err)
			}

			if len(users) != 2 {
				t.Fatalf("Expected 2 users, got %d", len(users))
			}

			if users[0].Name != "John Doe" {
				t.Errorf("Expected first user name 'John Doe', got '%s'", users[0].Name)
			}
			if users[1].Name != "Jane Smith" {
				t.Errorf("Expected second user name 'Jane Smith', got '%s'", users[1].Name)
			}
		})

		t.Run("Query with Rows Iterator", func(t *testing.T) {
			rows, err := sqlDriver.Query("SELECT id, name, email FROM test_users ORDER BY id")
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				var id int64
				var name, email string

				err := rows.Scan(&id, &name, &email)
				if err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}

				count++
				if count == 1 && name != "John Doe" {
					t.Errorf("Expected first row name 'John Doe', got '%s'", name)
				}
				if count == 2 && name != "Jane Smith" {
					t.Errorf("Expected second row name 'Jane Smith', got '%s'", name)
				}
			}

			if count != 2 {
				t.Errorf("Expected 2 rows, got %d", count)
			}
		})

		t.Run("Scan Database struct", func(t *testing.T) {
			// Insert a database record into the system database
			systemDB := app.DatabaseManager.SystemDatabase()
			defer systemDB.Close()

			_, err := systemDB.Exec(`
				INSERT INTO databases (database_id, name, created_at, updated_at) 
				VALUES (?, ?, ?, ?)
			`, []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeText, Value: []byte("test-db-123")},
				{Type: sqlite3.ParameterTypeText, Value: []byte("Test Database")},
				{Type: sqlite3.ParameterTypeText, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
				{Type: sqlite3.ParameterTypeText, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			})
			if err != nil {
				t.Fatalf("Failed to insert database record: %v", err)
			}

			// Create SQL driver for system database
			systemSQLDriver := database.NewSQLDriver(systemDB.GetConnection())

			// Scan into Database struct
			var dbRecord database.Database
			err = systemSQLDriver.ScanStruct(
				"SELECT database_id, name, created_at, updated_at FROM databases WHERE database_id = ?",
				&dbRecord,
				"test-db-123",
			)

			if err != nil {
				t.Fatalf("Failed to scan database: %v", err)
			}

			if dbRecord.DatabaseID != "test-db-123" {
				t.Errorf("Expected database_id 'test-db-123', got '%s'", dbRecord.DatabaseID)
			}
			if dbRecord.Name != "Test Database" {
				t.Errorf("Expected name 'Test Database', got '%s'", dbRecord.Name)
			}
		})

		t.Run("Error Handling", func(t *testing.T) {
			// Test query that returns no rows
			var user User
			err := sqlDriver.ScanStruct(
				"SELECT id, name, email, created_at FROM test_users WHERE email = ?",
				&user,
				"nonexistent@example.com",
			)

			if err != database.ErrNoRows {
				t.Errorf("Expected ErrNoRows, got %v", err)
			}

			// Test invalid SQL
			_, err = sqlDriver.Query("INVALID SQL STATEMENT")
			if err == nil {
				t.Error("Expected error for invalid SQL")
			}
		})
	})
}
