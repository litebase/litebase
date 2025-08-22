package sqlite3_test

import (
	"context"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestNewStatement(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	statement, errCode, err := sqlite3.NewStatement(ctx, con, "create table test (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	if errCode != 0 {
		t.Errorf("Expected error code 0, got %d", errCode)
	}

	if statement == nil {
		t.Error("Expected non-nil statement")
	}
}

func TestStatement_Bind(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, errCode, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	if errCode != 0 {
		t.Errorf("Expected error code 0, got %d", errCode)
	}

	if statement == nil {
		t.Error("Expected non-nil statement")
	}

	err = statement.Bind(sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("name"),
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatement_ClearBindings(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, errCode, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	if errCode != 0 {
		t.Errorf("Expected error code 0, got %d", errCode)
	}

	if statement == nil {
		t.Error("Expected non-nil statement")
	}

	err = statement.Bind(sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("name"),
	})

	if err != nil {
		t.Fatal(err)
	}

	err = statement.ClearBindings()

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatement_ColumnCount(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name, birthday) VALUES (?, ?)")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	err = statement.Bind(sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("name"),
	}, sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("birthday"),
	})

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err = sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	count := statement.ColumnCount()

	if count != 3 {
		t.Errorf("Expected 3 columns, got %d", count)
	}
}

func TestStatement_ColumnName(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	name := statement.ColumnName(1)

	if name != "name" {
		t.Errorf("Expected column name 'name', got '%s'", name)
	}
}

func TestStatement_ColumnNames(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	names := statement.ColumnNames()

	if len(names) != 3 {
		t.Errorf("Expected 3 column names, got %d", len(names))
	}

	if names[1] != "name" {
		t.Errorf("Expected column name 'name', got '%s'", names[1])
	}
}

func TestStatement_ColumnValue(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	_, err = con.Exec(ctx, "INSERT INTO names (name) VALUES (?)", sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("name"),
	})

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	result := sqlite3.NewResult()

	rc := statement.Step()

	if rc != sqlite3.SQLITE_ROW {
		t.Errorf("Expected SQLITE_ROW, got %d", rc)
	}

	value := statement.ColumnValue(
		result.GetBuffer(),
		sqlite3.ColumnTypeInteger,
		0,
	)

	// byte to int64
	intValue := binary.LittleEndian.Uint64(value)

	if intValue != 1 {
		t.Errorf("Expected value 1, got %d", intValue)
	}
}

func TestStatement_Exec(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	err = statement.Bind(sqlite3.StatementParameter{
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("name"),
	})

	if err != nil {
		t.Fatal(err)
	}

	result := sqlite3.NewResult()

	err = statement.Exec(result)

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatement_Finalize(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	err = statement.Finalize()

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatement_IsReadonly(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	if !statement.IsReadonly() {
		t.Error("Expected statement to be readonly")
	}
}

func TestStatementParameterCount(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			slog.Error("Error finalizing statement", "error", err)
		}
	}()

	count := statement.ParameterCount()

	if count != 0 {
		t.Errorf("Expected parameter count to be 0, got %d", count)
	}

	statement, _, err = sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	count = statement.ParameterCount()

	if count != 1 {
		t.Errorf("Expected parameter count to be 1, got %d", count)
	}
}

func TestStatementParameterIndex(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	index := statement.ParameterIndex("?")

	if index != 0 {
		t.Errorf("Expected parameter index to be 0, got %d", index)
	}

	_, err = con.Exec(ctx, "CREATE TABLE users (name TEXT, email TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err = sqlite3.NewStatement(ctx, con, "INSERT INTO users (name, email) VALUES (:name, :email)")

	if err != nil {
		t.Fatal(err)
	}

	index = statement.ParameterIndex(":name")

	if index != 1 {
		t.Errorf("Expected parameter index to be 1, got %d", index)
	}

	index = statement.ParameterIndex(":email")

	if index != 2 {
		t.Errorf("Expected parameter index to be 2, got %d", index)
	}

	if err := statement.Finalize(); err != nil {
		t.Errorf("Expected no error when finalizing statement, got: %v", err)
	}
}

func TestStatementParameterName(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table users (name TEXT, email TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO users (name, email) VALUES (:name, :email)")

	if err != nil {
		t.Fatal(err)
	}

	name := statement.ParameterName(1)

	if name != ":name" {
		t.Errorf("Expected parameter name to be ':name', got '%s'", name)
	}

	name = statement.ParameterName(2)

	if name != ":email" {
		t.Errorf("Expected parameter name to be ':email', got '%s'", name)
	}

	if err := statement.Finalize(); err != nil {
		t.Errorf("Expected no error when finalizing statement, got: %v", err)
	}
}

func TestStatementReset(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Errorf("Expected no error when finalizing statement, got: %v", err)
		}
	}()

	err = statement.Reset()

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatementSQL(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Errorf("Error closing connection: %v", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Errorf("Expected no error when finalizing statement, got: %v", err)
		}
	}()

	sql := statement.SQL()

	if string(sql) != "SELECT * FROM names" {
		t.Errorf("Expected SQL to be 'SELECT * FROM names', got '%s'", sql)
	}
}

func TestStatementStep(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Errorf("Expected no error when finalizing statement, got: %v", err)
		}
	}()

	rc := statement.Step()

	if rc != sqlite3.SQLITE_DONE {
		t.Errorf("Expected SQLITE_DONE, got %d", rc)
	}
}

func TestStatement_BindNamedParameters(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	// Create test table
	_, err = con.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	// Test named parameter binding
	statement, errCode, err := sqlite3.NewStatement(ctx, con, "INSERT INTO users (name, age, email) VALUES (:name, :age, :email)")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Error("Error finalizing statement:", err)
		}
	}()

	if errCode != 0 {
		t.Errorf("Expected error code 0, got %d", errCode)
	}

	// Bind named parameters
	err = statement.Bind(
		sqlite3.StatementParameter{
			Name:  ":name",
			Type:  sqlite3.ParameterTypeText,
			Value: []byte("John Doe"),
		},
		sqlite3.StatementParameter{
			Name:  ":age",
			Type:  sqlite3.ParameterTypeInteger,
			Value: int64(30),
		},
		sqlite3.StatementParameter{
			Name:  ":email",
			Type:  sqlite3.ParameterTypeText,
			Value: []byte("john@example.com"),
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	// Execute the statement
	result := sqlite3.NewResult()

	err = statement.Exec(result)

	if err != nil {
		t.Fatal(err)
	}

	// Verify the data was inserted by selecting it back
	selectStmt, _, err := sqlite3.NewStatement(ctx, con, "SELECT name, age, email FROM users WHERE name = :name")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := selectStmt.Finalize(); err != nil {
			slog.Error("Error finalizing select statement", "error", err)
		}
	}()

	err = selectStmt.Bind(sqlite3.StatementParameter{
		Name:  ":name",
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("John Doe"),
	})

	if err != nil {
		t.Fatal(err)
	}

	selectResult := sqlite3.NewResult()

	err = selectStmt.Exec(selectResult)

	if err != nil {
		t.Fatal(err)
	}

	if len(selectResult.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(selectResult.Rows))
	}

	if len(selectResult.Rows) > 0 {
		// Check name
		if string(selectResult.Rows[0][0].ColumnValue) != "John Doe" {
			t.Errorf("Expected name 'John Doe', got %s", string(selectResult.Rows[0][0].ColumnValue))
		}

		// Check age
		var age int64
		binary.LittleEndian.Uint64(selectResult.Rows[0][1].ColumnValue)
		expectedAge := int64(30)

		if age != expectedAge {
			age = int64(binary.LittleEndian.Uint64(selectResult.Rows[0][1].ColumnValue))
		}

		if age != expectedAge {
			t.Errorf("Expected age %d, got %d", expectedAge, age)
		}

		// Check email
		if string(selectResult.Rows[0][2].ColumnValue) != "john@example.com" {
			t.Errorf("Expected email 'john@example.com', got %s", string(selectResult.Rows[0][2].ColumnValue))
		}
	}
}

func TestStatement_BindNamedParametersWithDifferentFormats(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	// Create test table
	_, err = con.Exec(ctx, "CREATE TABLE test_params (id INTEGER PRIMARY KEY, value1 TEXT, value2 TEXT, value3 TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	// Test different parameter name formats
	testCases := []struct {
		name  string
		query string
		param string
	}{
		{"colon prefix", "INSERT INTO test_params (value1) VALUES (:param)", ":param"},
		{"at symbol", "INSERT INTO test_params (value1) VALUES (@param)", "@param"},
		{"dollar sign", "INSERT INTO test_params (value1) VALUES ($param)", "$param"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statement, _, err := sqlite3.NewStatement(ctx, con, tc.query)

			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				if err := statement.Finalize(); err != nil {
					t.Error("Error finalizing statement:", err)
				}
			}()

			err = statement.Bind(sqlite3.StatementParameter{
				Name:  tc.param,
				Type:  sqlite3.ParameterTypeText,
				Value: []byte("test_value"),
			})

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatement_BindNamedParameterNotFound(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	// Create test table
	_, err = con.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO test (name) VALUES (:name)")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Error("Error finalizing statement:", err)
		}
	}()

	// Try to bind a parameter that doesn't exist in the query
	err = statement.Bind(sqlite3.StatementParameter{
		Name:  ":nonexistent",
		Type:  sqlite3.ParameterTypeText,
		Value: []byte("test"),
	})

	if err == nil {
		t.Error("Expected error for nonexistent parameter, got nil")
	}

	expectedError := "named parameter not found: :nonexistent"

	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestStatement_BindMixedParameters(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	// Create test table
	_, err = con.Exec(ctx, "CREATE TABLE mixed_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Test mixing positional and named parameters
	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO mixed_test (name, age) VALUES (?, :age)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Error("Error finalizing statement:", err)
		}
	}()

	// Bind both positional (first parameter) and named (second parameter)
	err = statement.Bind(
		sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte("Alice"),
		},
		sqlite3.StatementParameter{
			Name:  ":age",
			Type:  sqlite3.ParameterTypeInteger,
			Value: int64(25),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	result := sqlite3.NewResult()
	err = statement.Exec(result)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStatement_ParameterIndex(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM sqlite_master WHERE name = :name AND type = :type")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Error("Error finalizing statement:", err)
		}
	}()

	// Test parameter index lookup
	nameIndex := statement.ParameterIndex(":name")
	if nameIndex == 0 {
		t.Error("Expected non-zero index for :name parameter")
	}

	typeIndex := statement.ParameterIndex(":type")
	if typeIndex == 0 {
		t.Error("Expected non-zero index for :type parameter")
	}

	// Test nonexistent parameter
	nonexistentIndex := statement.ParameterIndex(":nonexistent")
	if nonexistentIndex != 0 {
		t.Errorf("Expected index 0 for nonexistent parameter, got %d", nonexistentIndex)
	}

	// Test empty parameter name
	emptyIndex := statement.ParameterIndex("")
	if emptyIndex != 0 {
		t.Errorf("Expected index 0 for empty parameter name, got %d", emptyIndex)
	}
}

func TestStatement_ParameterName(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := con.Close(); err != nil {
			t.Error("Error closing connection:", err)
		}
	}()

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM sqlite_master WHERE name = :name AND type = :type")

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := statement.Finalize(); err != nil {
			t.Error("Error finalizing statement:", err)
		}
	}()

	// Test getting parameter names by index
	param1Name := statement.ParameterName(1)

	if param1Name != ":name" {
		t.Errorf("Expected parameter name ':name', got '%s'", param1Name)
	}

	param2Name := statement.ParameterName(2)

	if param2Name != ":type" {
		t.Errorf("Expected parameter name ':type', got '%s'", param2Name)
	}

	// Test invalid index
	invalidParamName := statement.ParameterName(999)

	if invalidParamName != "" {
		t.Errorf("Expected empty string for invalid index, got '%s'", invalidParamName)
	}
}
