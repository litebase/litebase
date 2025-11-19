package sqlite3_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestResult_ViewQueryColumns(t *testing.T) {
	// This test verifies that queries with aliases and literals
	// properly return column information

	ctx := t.Context()

	conn, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer conn.Close()

	// Create a test view
	_, err = conn.Exec(ctx, "CREATE VIEW test_view AS SELECT 1 as id, 'test' as name")

	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query sqlite_master like your actual query
	query := "SELECT name, 'main' as schema, sql as definition FROM sqlite_master WHERE type = 'view' ORDER BY name"

	result, err := conn.Exec(ctx, query)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check columns
	columns := result.ColumnNames()

	expectedColumns := []string{"name", "schema", "definition"}

	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d: %v", len(expectedColumns), len(columns), columns)
	}

	for i, expected := range expectedColumns {
		if i >= len(columns) {
			t.Errorf("Missing column at index %d: expected %s", i, expected)
			continue
		}

		if columns[i] != expected {
			t.Errorf("Column %d: expected %s, got %s", i, expected, columns[i])
		}
	}

	// Check that we got data
	if result.RowCount() == 0 {
		t.Error("Expected at least 1 row (test_view)")
	}
}

func TestResult_ColumnTypes(t *testing.T) {
	// Test that column type information is preserved for different scenarios

	ctx := t.Context()
	conn, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer conn.Close()

	testCases := []struct {
		name         string
		setup        string
		query        string
		expectedCols []string
	}{
		{
			name:         "literal_string_alias",
			query:        "SELECT 'test' as literal_col",
			expectedCols: []string{"literal_col"},
		},
		{
			name:         "literal_number_alias",
			query:        "SELECT 123 as num_col",
			expectedCols: []string{"num_col"},
		},
		{
			name:         "column_with_alias",
			setup:        "CREATE TABLE test1 (id INTEGER, name TEXT)",
			query:        "SELECT name as display_name FROM test1",
			expectedCols: []string{"display_name"},
		},
		{
			name:         "mixed_columns",
			setup:        "CREATE TABLE test2 (id INTEGER, name TEXT)",
			query:        "SELECT id, 'constant' as type, name as label FROM test2",
			expectedCols: []string{"id", "type", "label"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != "" {
				_, err := conn.Exec(ctx, tc.setup)
				if err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			result, err := conn.Exec(ctx, tc.query)

			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			columns := result.ColumnNames()

			if len(columns) != len(tc.expectedCols) {
				t.Errorf("Expected %d columns, got %d", len(tc.expectedCols), len(columns))
			}

			for i, expected := range tc.expectedCols {
				if i >= len(columns) {
					t.Errorf("Missing column %s", expected)
					continue
				}

				if columns[i] != expected {
					t.Errorf("Column %d: expected %s, got %s", i, expected, columns[i])
				}
			}
		})
	}
}

// TestResult_ZeroRowsColumnTypes tests that column types are correctly set
// even when a query returns zero rows
func TestResult_ZeroRowsColumnTypes(t *testing.T) {
	ctx := t.Context()

	conn, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}

	defer conn.Close()

	// Query that returns zero rows (no views exist in empty database)
	query := "SELECT name, 'main' as schema, sql as definition FROM sqlite_master WHERE type = 'view'"

	result, err := conn.Exec(ctx, query)

	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}

	// Verify we have 3 columns
	columns := result.ColumnNames()

	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}

	// Verify column names
	expectedNames := []string{"name", "schema", "definition"}

	for i, expected := range expectedNames {
		if columns[i] != expected {
			t.Errorf("Column %d: expected name %q, got %q", i, expected, columns[i])
		}
	}

	// Verify we have zero rows
	if result.RowCount() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.RowCount())
	}

	// CRITICAL: Verify that column types are set even with zero rows
	// This is the bug we're fixing - column types should be available
	// even when the query returns no data
	types := result.ColumnTypes

	if len(types) != 3 {
		t.Fatalf("Expected 3 column types, got %d", len(types))
	}

	// All three columns should have a defined type (not ColumnTypeUnknown=0)
	// SQLite should return TEXT (3) for all three columns
	for i, colType := range types {
		if colType == sqlite3.ColumnTypeUnknown {
			t.Errorf("Column %d (%s): expected defined type, got ColumnTypeUnknown (0)", i, expectedNames[i])
		}
	}
}
