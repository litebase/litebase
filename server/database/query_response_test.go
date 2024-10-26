package database_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"litebase/server/database"
	"litebase/server/sqlite3"
	"math"
	"testing"
)

func TestNewQueryResponse(t *testing.T) {
	queryResponse := database.NewQueryResponse(
		0,
		[]string{"id", "name"},
		[]byte("id"),
		0.01,
		1,
		[][]*sqlite3.Column{
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("1")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name1"))},
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("2")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name2"))},
		},
	)

	if queryResponse.Changes() != 0 {
		t.Fatalf("expected changes to be 0, got %v", queryResponse.Changes())
	}

	if queryResponse.Columns() == nil {
		t.Fatalf("expected columns to be not nil")
	}

	if bytes.Equal(queryResponse.Id(), []byte("id")) {
		t.Fatalf("expected id to be 'id', got %v", queryResponse.Id())
	}

	if queryResponse.Latency() != 0.01 {
		t.Fatalf("expected latency to be 0.01, got %v", queryResponse.Latency())
	}

	if queryResponse.LastInsertRowId() != 0 {
		t.Fatalf("expected last insert row ID to be 0, got %v", queryResponse.LastInsertRowId())
	}

	if queryResponse.RowCount() != 2 {
		t.Fatalf("expected row count to be 2, got %v", queryResponse.RowCount())
	}

	if queryResponse.Rows() == nil {
		t.Fatalf("expected rows to be not nil")
	}
}

func TestQueryResponseEncoding(t *testing.T) {
	queryResponse := database.NewQueryResponse(
		100,
		[]string{"id", "name"},
		[]byte("id"),
		0.01,
		2,
		[][]*sqlite3.Column{
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("1")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name1"))},
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("2")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name2"))},
		},
	)

	responseBuffer := bytes.NewBuffer(nil)
	rowsBuffer := bytes.NewBuffer(nil)
	columnsBuffer := bytes.NewBuffer(nil)

	data, err := queryResponse.Encode(responseBuffer, rowsBuffer, columnsBuffer)

	if err != nil {
		t.Fatal(err)
	}

	if data == nil {
		t.Fatalf("expected data to be not nil")
	}

	if len(data) == 0 {
		t.Fatalf("expected data to be not empty")
	}

	// Add more checks to ensure the encoded data is correct
	expectedVersion := byte(1)
	expectedChanges := 100
	expectedColumnCount := 2
	expectedRowCount := 2
	expectedLastInsertRowID := 2

	// Decode the data to verify its contents
	version := data[0]
	changes := int(binary.LittleEndian.Uint32(data[1:5]))
	latency := math.Float64frombits(binary.LittleEndian.Uint64(data[5:13]))
	columnCount := int(binary.LittleEndian.Uint32(data[13:17]))
	rowCount := int(binary.LittleEndian.Uint32(data[17:21]))
	lastInsertRowID := int(binary.LittleEndian.Uint32(data[21:25]))
	idLength := int(binary.LittleEndian.Uint32(data[25:29]))

	if version != expectedVersion {
		t.Fatalf("expected version %d, got %d", expectedVersion, version)
	}

	if changes != expectedChanges {
		t.Fatalf("expected changes %d, got %d", expectedChanges, changes)
	}

	if latency != 0.01 {
		t.Fatalf("expected latency 0.01, got %f", latency)
	}

	if columnCount != expectedColumnCount {
		t.Fatalf("expected column count %d, got %d", expectedColumnCount, columnCount)
	}

	if rowCount != expectedRowCount {
		t.Fatalf("expected row count %d, got %d", expectedRowCount, rowCount)
	}

	if lastInsertRowID != expectedLastInsertRowID {
		t.Fatalf("expected last insert row ID %d, got %d", expectedLastInsertRowID, lastInsertRowID)
	}

	if idLength != 2 {
		t.Fatalf("expected ID length 2, got %d", idLength)
	}

	id := data[29 : 29+idLength]

	if !bytes.Equal(id, []byte("id")) {
		t.Fatalf("expected ID 'id', got %s", id)
	}

	columnsLength := int(binary.LittleEndian.Uint32(data[29+idLength : 29+idLength+4]))

	if columnsLength != 14 {
		t.Fatalf("expected columns length 14, got %d", columnsLength)
	}

	columnsData := data[29+idLength+4 : 29+idLength+4+columnsLength]
	columns := make([]string, 0)

	columnDataOffset := 0

	for columnDataOffset < columnsLength {
		columnLength := int(binary.LittleEndian.Uint32(columnsData[columnDataOffset : columnDataOffset+4]))
		columns = append(columns, string(columnsData[columnDataOffset+4:columnDataOffset+4+columnLength]))
		columnDataOffset += 4 + columnLength
	}

	if len(columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(columns))
	}

	if columns[0] != "id" {
		t.Fatalf("expected column 1 to be 'id', got %s", columns[0])
	}

	if columns[1] != "name" {
		t.Fatalf("expected column 2 to be 'name', got %s", columns[1])
	}

	rowsData := data[29+idLength+4+columnsLength:]

	if len(rowsData) == 0 {
		t.Fatalf("expected rows data to be not empty")
	}

	rows := make([][]*sqlite3.Column, 0)

	rowsOffset := 0

	// Parse each row
	for rowsOffset < len(rowsData) {
		columns := make([]*sqlite3.Column, 0)
		rowLength := int(binary.LittleEndian.Uint32(rowsData[rowsOffset : rowsOffset+4]))
		rowData := rowsData[rowsOffset+4 : rowsOffset+4+rowLength]
		rowOffset := 0

		for rowOffset < len(rowData) {
			columnTypeInt := int(rowData[rowOffset])
			columnType := sqlite3.ColumnType(columnTypeInt)
			columnValueLength := 0
			var columnValue []byte

			// Read the length of the column value after the first byte
			columnValueLength = int(binary.LittleEndian.Uint32(rowData[rowOffset+1 : rowOffset+5]))

			columnValue = rowData[rowOffset+5 : rowOffset+5+columnValueLength]

			columns = append(columns, sqlite3.NewColumn(columnType, columnValue))
			rowOffset += 5 + columnValueLength
		}

		rows = append(rows, columns)
		rowsOffset += 4 + rowLength
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if len(rows[0]) != 2 {
		t.Fatalf("expected 2 columns in row 1, got %d", len(rows[0]))
	}

	if len(rows[1]) != 2 {
		t.Fatalf("expected 2 columns in row 2, got %d", len(rows[1]))
	}

	if rows[0][0].ColumnType != sqlite3.ColumnTypeText {
		t.Fatalf("expected column 1 in row 1 to be of type text")
	}

	if rows[0][1].ColumnType != sqlite3.ColumnTypeText {
		t.Fatalf("expected column 2 in row 1 to be of type text")
	}

	if rows[1][0].ColumnType != sqlite3.ColumnTypeText {
		t.Fatalf("expected column 1 in row 2 to be of type text")
	}

	if rows[1][1].ColumnType != sqlite3.ColumnTypeText {
		t.Fatalf("expected column 2 in row 2 to be of type text")
	}

}

func BenchmarkQueryResponseJsonEncoding(b *testing.B) {
	b.ReportAllocs()

	queryResponse := database.NewQueryResponse(
		0,
		[]string{"id", "name"},
		[]byte("id"),
		0.01,
		1,
		[][]*sqlite3.Column{
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("1")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name1"))},
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("2")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name2"))},
		},
	)

	b.ResetTimer()

	allocs := testing.AllocsPerRun(b.N, func() {
		queryResponse.Reset()

		queryResponse.SetChanges(0)
		queryResponse.SetColumns([]string{"id", "name"})
		queryResponse.SetId([]byte("id"))
		queryResponse.SetLatency(0.01)
		queryResponse.SetLastInsertRowId(1)
		queryResponse.SetRowCount(2)
		queryResponse.SetRows([][]*sqlite3.Column{
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("1")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name1"))},
			{sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("2")), sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte("name2"))},
		})

		_, err := json.Marshal(queryResponse)

		if err != nil {
			b.Fatal(err)
		}
	})

	// fail if allocation count is more than 2 per operation
	if allocs > 3 {
		b.Fatalf("unexpected allocation count: %v", allocs)
	}
}
