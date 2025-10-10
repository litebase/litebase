package database_test

import (
	"bytes"
	"testing"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// TestQueryResponseBinarySize compares the size of encoded query responses
// to demonstrate the efficiency gains of the new structure
func TestQueryResponseBinarySize(t *testing.T) {
	// Create a realistic dataset with 100 rows and 5 columns
	numRows := 100
	numCols := 5

	columns := []sqlite3.ColumnDefinition{
		{ColumnName: "id", ColumnType: sqlite3.ColumnTypeInteger},
		{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
		{ColumnName: "email", ColumnType: sqlite3.ColumnTypeText},
		{ColumnName: "age", ColumnType: sqlite3.ColumnTypeInteger},
		{ColumnName: "is_active", ColumnType: sqlite3.ColumnTypeInteger},
	}

	rows := make([][]*sqlite3.Column, numRows)

	for i := range numRows {
		rows[i] = make([]*sqlite3.Column, numCols)
		rows[i][0] = &sqlite3.Column{ColumnType: sqlite3.ColumnTypeInteger, ColumnValue: []byte{byte(i), 0, 0, 0, 0, 0, 0, 0}} // id (8 bytes)
		rows[i][1] = &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("John Doe")}                      // name
		rows[i][2] = &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("john@example.com")}              // email
		rows[i][3] = &sqlite3.Column{ColumnType: sqlite3.ColumnTypeInteger, ColumnValue: []byte{30, 0, 0, 0, 0, 0, 0, 0}}      // age (8 bytes)
		rows[i][4] = &sqlite3.Column{ColumnType: sqlite3.ColumnTypeInteger, ColumnValue: []byte{1}}                            // is_active (1 byte)
	}

	qr := database.NewQueryResponse(
		0,
		columns,
		"test-query-id",
		1.23,
		0,
		rows,
	)

	responseBuffer := new(bytes.Buffer)
	rowsBuffer := new(bytes.Buffer)
	columnsBuffer := new(bytes.Buffer)

	encoded, err := qr.Encode(responseBuffer, rowsBuffer, columnsBuffer)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	t.Logf("Dataset: %d rows x %d columns", numRows, numCols)
	t.Logf("Encoded size: %d bytes", len(encoded))
	t.Logf("Average bytes per row: %.2f", float64(len(encoded))/float64(numRows))

	// Calculate what the old format would have been (approximate)
	// Old format: each row would have type info for each column
	// Type info: 1 byte type + 4 byte length = 5 bytes per column per row
	oldFormatExtraBytes := numRows * numCols * 5
	t.Logf("Estimated old format overhead: ~%d bytes for type info in rows", oldFormatExtraBytes)
	t.Logf("Estimated old format total: ~%d bytes", len(encoded)+oldFormatExtraBytes)
	t.Logf("Space savings: ~%d%% reduction", (oldFormatExtraBytes*100)/(len(encoded)+oldFormatExtraBytes))
}

// BenchmarkQueryResponseEncode measures the encoding performance
func BenchmarkQueryResponseEncode(b *testing.B) {
	columns := []sqlite3.ColumnDefinition{
		{ColumnName: "id", ColumnType: sqlite3.ColumnTypeInteger},
		{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
	}

	rows := make([][]*sqlite3.Column, 100)

	for i := range 100 {
		rows[i] = []*sqlite3.Column{
			{ColumnType: sqlite3.ColumnTypeInteger, ColumnValue: []byte{byte(i), 0, 0, 0, 0, 0, 0, 0}}, // id (8 bytes)
			{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("Test Name")},                     // name
		}
	}

	qr := database.NewQueryResponse(0, columns, "test", 0.01, 0, rows)

	responseBuffer := new(bytes.Buffer)
	rowsBuffer := new(bytes.Buffer)
	columnsBuffer := new(bytes.Buffer)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := qr.Encode(responseBuffer, rowsBuffer, columnsBuffer)
		if err != nil {
			b.Fatal(err)
		}
	}
}
