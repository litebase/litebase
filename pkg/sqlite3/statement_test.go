package sqlite3_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestNewStatement(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer con.Close()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, errCode, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, errCode, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name, birthday) VALUES (?, ?)")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer statement.Finalize()

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

	defer con.Close()

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

	defer con.Close()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "INSERT INTO names (name) VALUES (?)")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer statement.Finalize()
}

func TestStatementParameterName(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer con.Close()

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

	defer statement.Finalize()
}

func TestStatementReset(t *testing.T) {
	ctx := context.Background()

	con, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatal(err)
	}

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

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

	defer con.Close()

	_, err = con.Exec(ctx, "create table names (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	statement, _, err := sqlite3.NewStatement(ctx, con, "SELECT * FROM names")

	if err != nil {
		t.Fatal(err)
	}

	defer statement.Finalize()

	rc := statement.Step()

	if rc != sqlite3.SQLITE_DONE {
		t.Errorf("Expected SQLITE_DONE, got %d", rc)
	}
}
