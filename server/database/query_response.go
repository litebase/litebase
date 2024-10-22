package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"litebase/server/sqlite3"
	"sync"
)

/*
A Query Response is a data structure that represents the result of a query to a
database.

| Offset     | Length | Name                | Description                                           |
|------------|--------|---------------------|-------------------------------------------------------|
| 0          | 4      | version             | The version of the query response.                    |
| 4          | 4      | changes             | The number of changes in the query.                   |
| 8          | 8      | latency             | The latency of the query in milliseconds.             |
| 16         | 4      | column_count        | The number of columns in the result set.              |
| 20         | 4      | row_count           | The number of rows in the result set.                 |
| 24         | 4      | last_insert_row_id  | The row ID of the last row inserted into the database.|
| 28         | 4      | id_length           | The length of the query ID.                           |
| 32         | n      | id                  | The unique identifier for the query.                  |
| 32 + n     | 4      | columns_length      | The length of the columns array.                      |
| 36 + n     | m      | columns             | The names of the columns in the result set.           |
| 40 + n + m | p      | rows                | The rows in the result set.                           |                       |
*/

// Buffer pool for reusing buffers
var queryResponseJsonBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type Row interface {
	string | int | float64 | bool
}

type QueryResponse struct {
	changes         int64
	columns         []string
	id              string
	latency         float64
	lastInsertRowId int64
	rowCount        int
	rows            [][]sqlite3.Column
}

type QueryJsonResponse struct {
	Status string         `json:"status"`
	Data   *QueryResponse `json:"data"`
}

func NewQueryResponse(
	changes int64,
	columns []string,
	id string,
	latency float64,
	lastInsertRowId int64,
	rows [][]sqlite3.Column,
) *QueryResponse {
	return &QueryResponse{
		changes:         changes,
		columns:         columns,
		id:              id,
		lastInsertRowId: lastInsertRowId,
		latency:         latency,
		rowCount:        len(rows),
		rows:            rows,
	}
}

func (qr *QueryResponse) Changes() int64 {
	return qr.changes
}

func (qr *QueryResponse) Columns() []string {
	return qr.columns
}

func (qr *QueryResponse) Encode(buffer *bytes.Buffer) ([]byte, error) {
	buffer.Reset()
	// Version
	binary.Write(buffer, binary.LittleEndian, uint32(1))
	// Changes
	binary.Write(buffer, binary.LittleEndian, uint32(qr.changes))
	// Latency
	binary.Write(buffer, binary.LittleEndian, qr.latency)
	// Column count
	binary.Write(buffer, binary.LittleEndian, uint32(len(qr.columns)))
	// Row count
	binary.Write(buffer, binary.LittleEndian, uint32(qr.rowCount))
	// Last insert row ID
	binary.Write(buffer, binary.LittleEndian, uint32(qr.lastInsertRowId))
	// ID length
	binary.Write(buffer, binary.LittleEndian, uint32(len(qr.id)))
	// ID
	buffer.Write([]byte(qr.id))

	// Encode the columns
	var columnsData bytes.Buffer

	for _, column := range qr.columns {
		// Column length
		binary.Write(&columnsData, binary.LittleEndian, uint32(len(column)))
		// Column
		columnsData.Write([]byte(column))
	}

	// Columns length
	binary.Write(buffer, binary.LittleEndian, uint32(columnsData.Len()))

	// Columns Data
	buffer.Write(columnsData.Bytes())

	// Rows
	var rowsData bytes.Buffer
	var columnBuffer bytes.Buffer

	for _, row := range qr.rows {
		rowData := make([]byte, 0)

		// Encode each row in the column
		for _, column := range row {
			columnData, err := column.Encode(&columnBuffer)

			if err != nil {
				return nil, err
			}

			rowData = append(rowData, columnData...)
		}

		// Write the row length
		binary.Write(&rowsData, binary.LittleEndian, uint32(len(rowData)))

		// Write the row data
		rowsData.Write(rowData)
	}

	// Write the rows data
	buffer.Write(rowsData.Bytes())

	return buffer.Bytes(), nil
}

func (qr *QueryResponse) JsonResponse() QueryJsonResponse {
	return QueryJsonResponse{
		Status: "success",
		Data:   qr,
	}
}

func (qr *QueryResponse) Id() string {
	return qr.id
}

func (qr *QueryResponse) LastInsertRowId() int64 {
	return qr.lastInsertRowId
}

func (qr *QueryResponse) Latency() float64 {
	return qr.latency
}

func (qr *QueryResponse) MarshalJSON() ([]byte, error) {
	type Alias QueryResponse
	buffer := queryResponseJsonBufferPool.Get().(*bytes.Buffer)
	defer queryResponseJsonBufferPool.Put(buffer)
	buffer.Reset()

	encoder := json.NewEncoder(buffer)

	err := encoder.Encode(&struct {
		*Alias
		Changes         int64              `json:"changes"`
		Columns         []string           `json:"columns"`
		ID              string             `json:"id"`
		Latency         float64            `json:"latency"`
		LastInsertRowID int64              `json:"lastInsertRowID"`
		RowCount        int                `json:"rowCount"`
		Rows            [][]sqlite3.Column `json:"rows"`
	}{
		Alias:           (*Alias)(qr),
		Changes:         qr.changes,
		Columns:         qr.columns,
		ID:              qr.id,
		Latency:         qr.latency,
		LastInsertRowID: qr.lastInsertRowId,
		RowCount:        qr.rowCount,
		Rows:            qr.rows,
	})

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (qr *QueryResponse) Reset() {
	qr.changes = 0
	qr.columns = qr.columns[:0]
	qr.id = ""
	qr.latency = 0
	qr.lastInsertRowId = 0
	qr.rowCount = 0
	qr.rows = qr.rows[:0]
}

func (qr *QueryResponse) RowCount() int {
	return qr.rowCount
}

func (qr *QueryResponse) Rows() [][]sqlite3.Column {
	return qr.rows
}

func (qr *QueryResponse) SetChanges(changes int64) {
	qr.changes = changes
}

func (qr *QueryResponse) SetColumns(columns []string) {
	if cap(qr.columns) >= len(columns) {
		// Reuse the existing slice's capacity
		qr.columns = qr.columns[:len(columns)]
	} else {
		// Allocate a new slice with the required capacity
		qr.columns = make([]string, len(columns))
	}

	copy(qr.columns, columns)
}

func (qr *QueryResponse) SetId(id string) {
	qr.id = id
}

func (qr *QueryResponse) SetLatency(latency float64) {
	qr.latency = latency
}

func (qr *QueryResponse) SetLastInsertRowId(lastInsertRowId int64) {
	qr.lastInsertRowId = lastInsertRowId
}

func (qr *QueryResponse) SetRowCount(rowCount int) {
	qr.rowCount = rowCount
}

func (qr *QueryResponse) SetRows(rows [][]sqlite3.Column) {
	if cap(qr.rows) >= len(rows) {
		// Reuse the existing slice's capacity
		qr.rows = qr.rows[:len(rows)]
	} else {
		// Allocate a new slice with the required capacity
		qr.rows = make([][]sqlite3.Column, len(rows))
	}

	for i, row := range rows {
		if cap(qr.rows[i]) >= len(row) {
			// Reuse the existing slice's capacity
			qr.rows[i] = qr.rows[i][:len(row)]
		} else {
			// Allocate a new slice with the required capacity
			qr.rows[i] = make([]sqlite3.Column, len(row))
		}

		copy(qr.rows[i], row)
	}
}

func (qr *QueryResponse) ToJSON() ([]byte, error) {
	return json.Marshal(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}

func (qr QueryResponse) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"changes":         qr.changes,
			"id":              qr.id,
			"latency":         qr.latency,
			"lastInsertRowID": qr.lastInsertRowId,
			"columns":         qr.columns,
			"rows":            qr.rows,
			"rowCount":        qr.rowCount,
		},
	}
}

func (qr *QueryResponse) WriteJson(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", " ")

	return encoder.Encode(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}
