package database_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestNewQueryResponse(t *testing.T) {
	columns := []sqlite3.ColumnDefinition{
		{ColumnName: "id", ColumnType: sqlite3.ColumnTypeText},
		{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
	}

	rows := [][]*sqlite3.Column{
		{&sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("1")}, &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("name1")}},
		{&sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("2")}, &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("name2")}},
	}

	queryResponse := database.NewQueryResponse(
		0,
		columns,
		"id",
		0.01,
		1,
		rows,
	)

	if queryResponse.Changes() != 0 {
		t.Fatalf("expected changes to be 0, got %v", queryResponse.Changes())
	}

	if queryResponse.Columns() == nil {
		t.Fatalf("expected columns to be not nil")
	}

	if len(queryResponse.Columns()) != 2 {
		t.Fatalf("expected 2 columns, got %v", len(queryResponse.Columns()))
	}

	if queryResponse.Id() != "id" {
		t.Fatalf("expected id to be %v, got %v", "id", queryResponse.Id())
	}

	if queryResponse.Latency() != 0.01 {
		t.Fatalf("expected latency to be 0.01, got %v", queryResponse.Latency())
	}

	if queryResponse.LastInsertRowId() != 1 {
		t.Fatalf("expected last insert row ID to be 1, got %v", queryResponse.LastInsertRowId())
	}

	if queryResponse.RowCount() != 2 {
		t.Fatalf("expected row count to be 2, got %v", queryResponse.RowCount())
	}

	if queryResponse.Rows() == nil {
		t.Fatalf("expected rows to be not nil")
	}
}

func TestQueryResponseEncodingWithResults(t *testing.T) {
	// Setup test data
	id := "query123"
	transactionID := "txn456"
	columns := []sqlite3.ColumnDefinition{
		{ColumnName: "col1", ColumnType: sqlite3.ColumnTypeText},
		{ColumnName: "col2", ColumnType: sqlite3.ColumnTypeInteger},
	}

	rows := [][]*sqlite3.Column{
		{&sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("foo")}, &sqlite3.Column{ColumnType: sqlite3.ColumnTypeInteger, ColumnValue: []byte("42")}},
	}

	qr := database.NewQueryResponse(1, columns, id, 12.34, 99, rows)
	qr.SetTransactionID(transactionID)

	responseBuffer := new(bytes.Buffer)
	rowsBuffer := new(bytes.Buffer)
	columnsBuffer := new(bytes.Buffer)

	encoded, err := qr.Encode(responseBuffer, rowsBuffer, columnsBuffer)

	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	offset := 0

	// Version
	if encoded[offset] != 1 {
		t.Errorf("expected version 1, got %d", encoded[offset])
	}

	offset += 1

	// ID length
	idLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if idLen != len(id) {
		t.Errorf("expected id length %d, got %d", len(id), idLen)
	}

	offset += 4

	// ID
	if string(encoded[offset:offset+idLen]) != string(id) {
		t.Errorf("expected id %q, got %q", id, encoded[offset:offset+idLen])
	}

	offset += idLen

	// Transaction ID length
	txnIdLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if txnIdLen != len(transactionID) {
		t.Errorf("expected transaction id length %d, got %d", len(transactionID), txnIdLen)
	}

	offset += 4

	// Transaction ID
	if string(encoded[offset:offset+txnIdLen]) != string(transactionID) {
		t.Errorf("expected transaction id %q, got %q", transactionID, encoded[offset:offset+txnIdLen])
	}

	offset += txnIdLen

	// Error: should not be present, so next is result set
	// Changes
	changes := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if changes != 1 {
		t.Errorf("expected changes 1, got %d", changes)
	}

	offset += 4

	// Latency
	latency := math.Float64frombits(binary.LittleEndian.Uint64(encoded[offset : offset+8]))

	if latency != 12.34 {
		t.Errorf("expected latency 12.34, got %f", latency)
	}

	offset += 8

	// Column count
	colCount := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if colCount != len(columns) {
		t.Errorf("expected column count %d, got %d", len(columns), colCount)
	}

	offset += 4

	// Row count
	rowCount := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if rowCount != len(rows) {
		t.Errorf("expected row count %d, got %d", len(rows), rowCount)
	}

	offset += 4

	// Last insert row id
	lastInsertRowId := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))

	if lastInsertRowId != 99 {
		t.Errorf("expected last insert row id 99, got %d", lastInsertRowId)
	}

	offset += 4

	// Columns length
	columnsLength := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))
	offset += 4

	// Columns data - now includes both names and types
	colOffset := offset
	decodedColumns := make(map[string]sqlite3.ColumnType)

	for range colCount {
		// Column name length
		colNameLen := int(binary.LittleEndian.Uint32(encoded[colOffset : colOffset+4]))
		colOffset += 4

		// Column name
		colName := string(encoded[colOffset : colOffset+colNameLen])
		colOffset += colNameLen

		// Column type (4 bytes as int32)
		colType := sqlite3.ColumnType(binary.LittleEndian.Uint32(encoded[colOffset : colOffset+4]))
		colOffset += 4

		decodedColumns[colName] = colType
	}

	// Verify we consumed the right amount of columns data
	if colOffset-offset != columnsLength {
		t.Errorf("columns length mismatch: expected %d, got %d", columnsLength, colOffset-offset)
	}

	offset = colOffset

	// Rows: check row count and row data length
	for range rowCount {
		rowLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))
		offset += 4
		// We could decode the row data here if needed
		offset += rowLen
	}

	// Should have consumed all bytes
	if offset != len(encoded) {
		t.Errorf("did not consume all bytes, offset=%d, len(encoded)=%d", offset, len(encoded))
	}
}

func TestQueryResponseEncodingWithError(t *testing.T) {
	id := "query123"
	transactionID := "txn456"
	errorMsg := "something went wrong"
	qr := database.NewQueryResponse(0, nil, id, 0, 0, nil)
	qr.SetTransactionID(transactionID)
	qr.SetError(errorMsg)

	responseBuffer := new(bytes.Buffer)
	rowsBuffer := new(bytes.Buffer)
	columnsBuffer := new(bytes.Buffer)

	encoded, err := qr.Encode(responseBuffer, rowsBuffer, columnsBuffer)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	offset := 0

	// Version
	if encoded[offset] != 1 {
		t.Errorf("expected version 1, got %d", encoded[offset])
	}
	offset += 1

	// ID length
	idLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))
	if idLen != len(id) {
		t.Errorf("expected id length %d, got %d", len(id), idLen)
	}
	offset += 4

	// ID
	if string(encoded[offset:offset+idLen]) != string(id) {
		t.Errorf("expected id %q, got %q", id, encoded[offset:offset+idLen])
	}
	offset += idLen

	// Transaction ID length
	txnIdLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))
	if txnIdLen != len(transactionID) {
		t.Errorf("expected transaction id length %d, got %d", len(transactionID), txnIdLen)
	}
	offset += 4

	// Transaction ID
	if string(encoded[offset:offset+txnIdLen]) != string(transactionID) {
		t.Errorf("expected transaction id %q, got %q", transactionID, encoded[offset:offset+txnIdLen])
	}
	offset += txnIdLen

	// Error length
	errorLen := int(binary.LittleEndian.Uint32(encoded[offset : offset+4]))
	if errorLen != len(errorMsg) {
		t.Errorf("expected error length %d, got %d", len(errorMsg), errorLen)
	}
	offset += 4

	// Error message
	if string(encoded[offset:offset+errorLen]) != errorMsg {
		t.Errorf("expected error message %q, got %q", errorMsg, encoded[offset:offset+errorLen])
	}
	offset += errorLen

	// Should have consumed all bytes
	if offset != len(encoded) {
		t.Errorf("did not consume all bytes, offset=%d, len(encoded)=%d", offset, len(encoded))
	}
}

func BenchmarkQueryResponseJsonEncoding(b *testing.B) {
	b.ReportAllocs()

	columns := []sqlite3.ColumnDefinition{
		{ColumnName: "id", ColumnType: sqlite3.ColumnTypeText},
		{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
	}

	rows := [][]*sqlite3.Column{
		{&sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("1")}, &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("name1")}},
		{&sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("2")}, &sqlite3.Column{ColumnType: sqlite3.ColumnTypeText, ColumnValue: []byte("name2")}},
	}

	queryResponse := database.NewQueryResponse(
		0,
		columns,
		"id",
		0.01,
		1,
		rows,
	)

	b.ResetTimer()

	allocs := testing.AllocsPerRun(b.N, func() {
		queryResponse.Reset()

		queryResponse.SetChanges(0)
		queryResponse.SetColumns([]sqlite3.ColumnDefinition{
			{ColumnName: "id", ColumnType: sqlite3.ColumnTypeText},
			{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
		})
		queryResponse.SetID("id")
		queryResponse.SetLatency(0.01)
		queryResponse.SetLastInsertRowID(1)
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

	// With the new structure (map for columns, byte arrays for rows),
	// we expect more allocations than before, but it's worth it for the efficiency gain
	// in the binary protocol. Accept up to 30 allocations.
	if allocs > 30 {
		b.Fatalf("unexpected allocation count: %v", allocs)
	}
}
