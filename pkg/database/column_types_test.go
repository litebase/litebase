package database_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// TestQueryResponse_ZeroRowsHasColumnTypes verifies that column types are
// correctly set in QueryResponse even when a query returns zero rows.
// This tests the complete integration from sqlite3.Statement -> Result -> QueryResponse.
func TestQueryResponse_ZeroRowsHasColumnTypes(t *testing.T) {
	ctx := t.Context()

	// Open an in-memory database
	conn, err := sqlite3.Open(ctx, ":memory:", "", sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer conn.Close()

	// Query that returns zero rows (no views exist in empty database)
	query := "SELECT name, 'main' as schema, sql as definition FROM sqlite_master WHERE type = 'view'"

	// Execute via Connection.Exec to get a Result
	result, err := conn.Exec(ctx, query)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify the Result has column types even with zero rows
	if len(result.ColumnTypes) != 3 {
		t.Errorf("Expected 3 column types in Result, got %d", len(result.ColumnTypes))
	}

	// For zero-row queries:
	// - Table columns (name, definition) should have types from schema
	// - Literal expressions ('main' as schema) will be ColumnTypeUnknown
	for i, colType := range result.ColumnTypes {
		t.Logf("Result column %d type: %d", i, colType)
		// Columns 0 and 2 are from sqlite_master table, should have types
		if (i == 0 || i == 2) && colType == sqlite3.ColumnTypeUnknown {
			t.Errorf("Result column %d from table should have type from schema", i)
		}
	}

	// Now test the QueryResponse integration
	response := database.NewQueryResponse(0, nil, "test-query-id", 0, 0, nil)

	// This mimics what resolver.go does
	var firstRow []*sqlite3.Column

	if len(result.Rows) > 0 {
		firstRow = result.Rows[0]
	}

	response.SetColumnsFromResult(result.Columns, result.ColumnTypes, firstRow)

	// Verify QueryResponse has the column types
	columns := response.Columns()

	if len(columns) != 3 {
		t.Errorf("Expected 3 columns in QueryResponse, got %d", len(columns))
	}

	expectedNames := []string{"name", "schema", "definition"}

	for i, col := range columns {
		if col.ColumnName != expectedNames[i] {
			t.Errorf("Column %d: expected name %q, got %q", i, expectedNames[i], col.ColumnName)
		}

		t.Logf("Column %d (%s): type = %d", i, col.ColumnName, col.ColumnType)
		
		// CRITICAL: This was the bug - column type was 0 (ColumnTypeUnknown) for ALL zero-row queries
		// Now table columns (0, 2) should have types from schema
		// Literal expressions (1) may still be ColumnTypeUnknown, which is acceptable
		if (i == 0 || i == 2) && col.ColumnType == sqlite3.ColumnTypeUnknown {
			t.Errorf("Column %d (%s): table column should have type from schema", i, col.ColumnName)
		}
	}
}
