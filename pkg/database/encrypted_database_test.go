package database_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
)

func TestEncryptedDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServerWithEncryption(t)
		defer server.Shutdown()

		app := server.App

		t.Run("CreateAndQuery", func(t *testing.T) {
			db := test.MockEncryptedDatabase(app)

			// Get a connection to the encrypted database
			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(con)

			// Create a table
			_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", nil)

			if err != nil {
				t.Fatalf("Failed to create table in encrypted database: %v", err)
			}

			// Insert data
			_, err = con.GetConnection().Exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')", nil)

			if err != nil {
				t.Fatalf("Failed to insert into encrypted database: %v", err)
			}

			_, err = con.GetConnection().Exec("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')", nil)

			if err != nil {
				t.Fatalf("Failed to insert second row: %v", err)
			}

			// Query data
			res, err := con.GetConnection().Exec("SELECT name, email FROM users ORDER BY id", nil)

			if err != nil {
				t.Fatalf("Failed to query encrypted database: %v", err)
			}

			if len(res.Rows) != 2 {
				t.Errorf("Expected 2 rows, got %d", len(res.Rows))
			}

			if string(res.Rows[0][0].Text()) != "Alice" {
				t.Errorf("Expected first user to be 'Alice', got '%s'", string(res.Rows[0][0].Text()))
			}

			// Update data
			_, err = con.GetConnection().Exec("UPDATE users SET email = 'alice.smith@example.com' WHERE name = 'Alice'", nil)

			if err != nil {
				t.Fatalf("Failed to update encrypted database: %v", err)
			}

			// Verify update
			res, err = con.GetConnection().Exec("SELECT email FROM users WHERE name = 'Alice'", nil)

			if err != nil {
				t.Fatalf("Failed to query after update: %v", err)
			}

			if string(res.Rows[0][0].Text()) != "alice.smith@example.com" {
				t.Errorf("Expected updated email, got '%s'", string(res.Rows[0][0].Text()))
			}

			// Delete data
			_, err = con.GetConnection().Exec("DELETE FROM users WHERE name = 'Bob'", nil)

			if err != nil {
				t.Fatalf("Failed to delete from encrypted database: %v", err)
			}

			// Verify delete
			res, err = con.GetConnection().Exec("SELECT COUNT(*) FROM users", nil)

			if err != nil {
				t.Fatalf("Failed to count after delete: %v", err)
			}

			if res.Rows[0][0].Int64() != 1 {
				t.Errorf("Expected 1 row after delete, got %d", res.Rows[0][0].Int64())
			}
		})

		t.Run("CreateBranch", func(t *testing.T) {
			// Create an encrypted database
			db := test.MockEncryptedDatabase(app)

			// Create table and insert data in primary branch
			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection: %v", err)
			}

			_, err = con.GetConnection().Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)", nil)

			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to checkpoint: %v", err)
			}

			_, err = con.GetConnection().Exec("INSERT INTO products (name, price) VALUES ('Widget', 19.99)", nil)

			if err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to checkpoint: %v", err)
			}

			app.DatabaseManager.ConnectionManager().Release(con)

			databaseInstance, err := app.DatabaseManager.Get(db.DatabaseID)

			if err != nil {
				t.Fatalf("Failed to retrieve database instance: %v", err)
			}

			if databaseInstance == nil {
				t.Fatal("Failed to retrieve database instance")
			}

			// Create a branch from the encrypted database
			newBranch, err := databaseInstance.CreateBranch("feature", "main")

			if err != nil {
				t.Fatalf("Failed to create branch from encrypted database: %v", err)
			}

			// Verify the branch has encryption enabled
			if !newBranch.Settings.Encrypted {
				t.Error("Expected new branch to have encryption enabled")
			}

			primaryBranch, err := databaseInstance.PrimaryBranch()

			if err != nil {
				t.Fatalf("Failed to retrieve primary branch: %v", err)
			}

			if newBranch.Settings.DataEncryptionKeyHash != primaryBranch.Settings.DataEncryptionKeyHash {
				t.Error("Expected new branch to have same encryption key hash as parent")
			}

			// Verify we can read data in the new branch
			branchCon, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, newBranch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection to new branch: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(branchCon)

			res, err := branchCon.GetConnection().Exec("SELECT name, price FROM products", nil)

			if err != nil {
				t.Fatalf("Failed to query new branch: %v", err)
			}

			if len(res.Rows) != 1 {
				t.Errorf("Expected 1 row in new branch, got %d", len(res.Rows))
			}

			if string(res.Rows[0][0].Text()) != "Widget" {
				t.Errorf("Expected product name 'Widget', got '%s'", string(res.Rows[0][0].Text()))
			}

			// Insert data in the new branch
			_, err = branchCon.GetConnection().Exec("INSERT INTO products (name, price) VALUES ('Gadget', 29.99)", nil)

			if err != nil {
				t.Fatalf("Failed to insert into branch: %v", err)
			}

			// Verify new data is in branch
			res, err = branchCon.GetConnection().Exec("SELECT COUNT(*) FROM products", nil)

			if err != nil {
				t.Fatalf("Failed to count in branch: %v", err)
			}

			if res.Rows[0][0].Int64() != 2 {
				t.Errorf("Expected 2 rows in branch, got %d", res.Rows[0][0].Int64())
			}
		})

		t.Run("MultipleOperations", func(t *testing.T) {
			// Create an encrypted database
			db, err := database.CreateDatabase(app.DatabaseManager, "test_encrypted_multi", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatal(err)
			}

			// Enable encryption
			err = primaryBranch.SetEncryptionSettings(true, app.Config.DataEncryptionKeyHash)

			if err != nil {
				t.Fatalf("Failed to enable encryption: %v", err)
			}

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, primaryBranch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(con)

			// Create multiple tables
			_, err = con.GetConnection().Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)", nil)

			if err != nil {
				t.Fatalf("Failed to create orders table: %v", err)
			}

			_, err = con.GetConnection().Exec("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT, price REAL)", nil)

			if err != nil {
				t.Fatalf("Failed to create order_items table: %v", err)
			}

			// Insert related data
			_, err = con.GetConnection().Exec("INSERT INTO orders (total) VALUES (100.00)", nil)

			if err != nil {
				t.Fatalf("Failed to insert order: %v", err)
			}

			_, err = con.GetConnection().Exec("INSERT INTO order_items (order_id, item, price) VALUES (1, 'Item A', 50.00)", nil)

			if err != nil {
				t.Fatalf("Failed to insert order item: %v", err)
			}

			_, err = con.GetConnection().Exec("INSERT INTO order_items (order_id, item, price) VALUES (1, 'Item B', 50.00)", nil)

			if err != nil {
				t.Fatalf("Failed to insert second order item: %v", err)
			}

			// Perform JOIN query
			res, err := con.GetConnection().Exec("SELECT o.id, o.total, COUNT(oi.id) as item_count FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id GROUP BY o.id", nil)

			if err != nil {
				t.Fatalf("Failed to perform JOIN query: %v", err)
			}

			if len(res.Rows) != 1 {
				t.Errorf("Expected 1 order, got %d", len(res.Rows))
			}

			if res.Rows[0][2].Int64() != 2 {
				t.Errorf("Expected 2 items, got %d", res.Rows[0][2].Int64())
			}

			// Create index
			_, err = con.GetConnection().Exec("CREATE INDEX idx_order_items_order_id ON order_items(order_id)", nil)

			if err != nil {
				t.Fatalf("Failed to create index on encrypted database: %v", err)
			}

			// Verify index works with query
			res, err = con.GetConnection().Exec("SELECT item FROM order_items WHERE order_id = 1 ORDER BY item", nil)

			if err != nil {
				t.Fatalf("Failed to query with index: %v", err)
			}

			if len(res.Rows) != 2 {
				t.Errorf("Expected 2 items, got %d", len(res.Rows))
			}
		})

		t.Run("WithCheckpoints", func(t *testing.T) {
			// Create an encrypted database
			db, err := database.CreateDatabase(app.DatabaseManager, "test_encrypted_checkpoints", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatal(err)
			}

			// Enable encryption
			err = primaryBranch.SetEncryptionSettings(true, app.Config.DataEncryptionKeyHash)

			if err != nil {
				t.Fatalf("Failed to enable encryption: %v", err)
			}

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, primaryBranch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(con)

			// Create table
			_, err = con.GetConnection().Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)", nil)

			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Force checkpoint after creating table
			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, primaryBranch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to checkpoint after table creation: %v", err)
			}

			// Insert data across multiple checkpoints
			for i := 1; i <= 5; i++ {
				_, err = con.GetConnection().Exec(fmt.Sprintf("INSERT INTO logs (message) VALUES ('Log entry %d')", i), nil)

				if err != nil {
					t.Fatalf("Failed to insert log entry %d: %v", i, err)
				}

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, primaryBranch.DatabaseBranchID)

				if err != nil {
					t.Fatalf("Failed to checkpoint after insert %d: %v", i, err)
				}
			}

			// Verify all data is present
			res, err := con.GetConnection().Exec("SELECT COUNT(*) FROM logs", nil)

			if err != nil {
				t.Fatalf("Failed to count logs: %v", err)
			}

			if res.Rows[0][0].Int64() != 5 {
				t.Errorf("Expected 5 log entries, got %d", res.Rows[0][0].Int64())
			}

			// Verify data integrity
			res, err = con.GetConnection().Exec("SELECT message FROM logs ORDER BY id", nil)

			if err != nil {
				t.Fatalf("Failed to query logs: %v", err)
			}

			for i := range 5 {
				expected := fmt.Sprintf("Log entry %d", i+1)

				if string(res.Rows[i][0].Text()) != expected {
					t.Errorf("Expected log message '%s', got '%s'", expected, string(res.Rows[i][0].Text()))
				}
			}
		})
	})
}
