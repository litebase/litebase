package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"sync"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/sqlite3"
)

/*
A Query Response is a data structure that represents the result of a query to a
database. Each response will start with the following data:

| Offset      | Length | Name                  | Description                                         |
|-------------|--------|-----------------------|-----------------------------------------------------|
| 0           | 1      | version               | The version of the query response.                  |
| 1           | 4      | id_length             | The length of the query ID.                         |
| 5           | n      | id                    | The unique identifier for the query.                |
| 5 + n       | 4      | transaction_id_length | The length of the transaction ID.                   |
| 9 + n       | m      | transaction_id        | The unique identifier for the transaction.          |

Following the transaction ID, the response will contain either an error or a
result set.

For an error response, the format is:

| Offset        | Length | Name         | Description                        |
|---------------|--------|--------------|------------------------------------|
| 13 + n + m    | 4      | error_length | The length of the error message.   |
| 17 + n + m    | k      | error        | The error message, if any.         |

For a result set response, the format is:

| Offset          | Length | Name                | Description                                           |
|-----------------|--------|---------------------|-------------------------------------------------------|
| 13 + n + m      | 4      | changes             | The number of changes in the query.                   |
| 17 + n + m      | 8      | latency             | The latency of the query in milliseconds.             |
| 25 + n + m      | 4      | column_count        | The number of columns in the result set.              |
| 29 + n + m      | 4      | row_count           | The number of rows in the result set.                 |
| 33 + n + m      | 4      | last_insert_row_id  | The row ID of the last row inserted into the database.|
| 37 + n + m      | 4      | columns_length      | The length of the columns array.                      |
| 41 + n + m      | p      | columns             | The names of the columns in the result set.           |
| 41 + n + m + p  | q      | rows                | The rows in the result set.                           |
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
	err             string
	id              []byte
	latency         float64
	lastInsertRowId int64
	rowCount        int
	rows            [][]*sqlite3.Column
	transactionId   []byte
	walSequence     int64
	walTimestamp    int64
}

type QueryJsonResponse struct {
	Status string         `json:"status"`
	Data   *QueryResponse `json:"data"`
}

func NewQueryResponse(
	changes int64,
	columns []string,
	id []byte,
	latency float64,
	lastInsertRowId int64,
	rows [][]*sqlite3.Column,
) *QueryResponse {
	return &QueryResponse{
		changes:         changes,
		columns:         columns,
		err:             "",
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

func (qr *QueryResponse) Encode(responseBuffer, rowsBuffer, columnsBuffer *bytes.Buffer) ([]byte, error) {
	responseBuffer.Reset()
	// Version
	responseBuffer.WriteByte(uint8(1))
	// ID length
	var idLengthBytes [4]byte

	idLenUint32, err := utils.SafeIntToUint32(len(qr.id))

	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint32(idLengthBytes[:], idLenUint32)
	responseBuffer.Write(idLengthBytes[:])
	// ID
	responseBuffer.Write([]byte(qr.id))
	// Transaction ID length
	var transactionIdLengthBytes [4]byte

	transactionIDLenUint32, err := utils.SafeIntToUint32(len(qr.transactionId))

	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint32(transactionIdLengthBytes[:], transactionIDLenUint32)
	responseBuffer.Write(transactionIdLengthBytes[:])

	// Transaction ID
	responseBuffer.Write(qr.transactionId)

	if len(qr.err) > 0 {
		// Error length
		var errorLengthBytes [4]byte

		errorLenUint32, err := utils.SafeIntToUint32(len(qr.err))

		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(errorLengthBytes[:], errorLenUint32)

		// Write the error length
		responseBuffer.Write(errorLengthBytes[:])

		// Write the error
		responseBuffer.Write([]byte(qr.err))
	} else {
		// Changes
		var changesBytes [4]byte
		changesUint32, err := utils.SafeInt64ToUint32(qr.changes)

		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(changesBytes[:], changesUint32)
		responseBuffer.Write(changesBytes[:])
		// Latency
		var latencyBytes [8]byte
		binary.LittleEndian.PutUint64(latencyBytes[:], math.Float64bits(qr.latency))
		responseBuffer.Write(latencyBytes[:])
		// Column count
		var columnCountBytes [4]byte

		columnCountUint32, err := utils.SafeInt64ToUint32(int64(len(qr.columns)))

		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(columnCountBytes[:], columnCountUint32)
		responseBuffer.Write(columnCountBytes[:])

		// Row count
		var rowCountBytes [4]byte

		rowCountUint32, err := utils.SafeInt64ToUint32(int64(qr.rowCount))

		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(rowCountBytes[:], rowCountUint32)
		responseBuffer.Write(rowCountBytes[:])

		// Last insert row ID
		var lastInsertRowIdBytes [4]byte

		uint32LastInsertRowId, err := utils.SafeInt64ToUint32(qr.lastInsertRowId)

		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(lastInsertRowIdBytes[:], uint32LastInsertRowId)
		responseBuffer.Write(lastInsertRowIdBytes[:])

		// Calculate the length of the columns data to be written and write it
		// to the response buffer before writing the columns data.
		columnDataLength := 0

		for _, column := range qr.columns {
			columnDataLength = columnDataLength + 4 + len(column)
		}

		// Columns length
		var columnsLengthBytes [4]byte

		uint32ColumnsLength, err := utils.SafeInt64ToUint32(int64(columnDataLength))

		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(columnsLengthBytes[:], uint32ColumnsLength)
		responseBuffer.Write(columnsLengthBytes[:])

		// Encode the columns
		var columnLengthBytes [4]byte

		for _, column := range qr.columns {
			// Column length
			columnLengthUint32, err := utils.SafeInt64ToUint32(int64(len(column)))

			if err != nil {
				return nil, err
			}

			binary.LittleEndian.PutUint32(columnLengthBytes[:], columnLengthUint32)
			responseBuffer.Write(columnLengthBytes[:])

			// Column
			responseBuffer.Write([]byte(column))
		}

		// Rows
		for _, row := range qr.rows {
			rowsBuffer.Reset()

			// Encode each row in the column
			for _, column := range row {
				err := column.Encode(columnsBuffer)

				if err != nil {
					return nil, err
				}

				rowsBuffer.Write(columnsBuffer.Bytes())
			}

			// Write the row length
			var rowLengthBytes [4]byte

			rowsLenUint32, err := utils.SafeIntToUint32(rowsBuffer.Len())

			if err != nil {
				return nil, err
			}

			binary.LittleEndian.PutUint32(rowLengthBytes[:], rowsLenUint32)
			responseBuffer.Write(rowLengthBytes[:])

			// Write the row data
			responseBuffer.Write(rowsBuffer.Bytes())
		}
	}

	return responseBuffer.Bytes(), nil
}

func (qr *QueryResponse) Error() string {
	return qr.err
}

func (qr *QueryResponse) JsonResponse() QueryJsonResponse {
	return QueryJsonResponse{
		Status: "success",
		Data:   qr,
	}
}

func (qr *QueryResponse) Id() []byte {
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
		Changes         int64               `json:"changes"`
		Columns         []string            `json:"columns"`
		ID              []byte              `json:"id"`
		Latency         float64             `json:"latency"`
		LastInsertRowID int64               `json:"last_insert_row_id"`
		RowCount        int                 `json:"row_count"`
		Rows            [][]*sqlite3.Column `json:"rows"`
		TransactionID   []byte              `json:"transaction_id"`
	}{
		Alias:           (*Alias)(qr),
		Changes:         qr.changes,
		Columns:         qr.columns,
		ID:              qr.id,
		Latency:         qr.latency,
		LastInsertRowID: qr.lastInsertRowId,
		RowCount:        qr.rowCount,
		Rows:            qr.rows,
		TransactionID:   qr.transactionId,
	})

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (qr *QueryResponse) Reset() {
	qr.changes = 0
	qr.columns = qr.columns[:0]
	qr.err = ""
	qr.id = []byte{}
	qr.latency = 0
	qr.lastInsertRowId = 0
	qr.rowCount = 0
	qr.rows = qr.rows[:0]
	qr.transactionId = []byte{}
}

func (qr *QueryResponse) RowCount() int {
	return qr.rowCount
}

func (qr *QueryResponse) Rows() [][]*sqlite3.Column {
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

func (qr *QueryResponse) SetError(err string) {
	qr.err = err
}

func (qr *QueryResponse) SetId(id []byte) {
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

func (qr *QueryResponse) SetRows(rows [][]*sqlite3.Column) {
	if cap(qr.rows) >= len(rows) {
		// Reuse the existing slice's capacity
		qr.rows = qr.rows[:len(rows)]
	} else {
		// Allocate a new slice with the required capacity
		qr.rows = make([][]*sqlite3.Column, len(rows))
	}

	for i, row := range rows {
		if cap(qr.rows[i]) >= len(row) {
			// Reuse the existing slice's capacity
			qr.rows[i] = qr.rows[i][:len(row)]
		} else {
			// Allocate a new slice with the required capacity
			qr.rows[i] = make([]*sqlite3.Column, len(row))
		}

		copy(qr.rows[i], row)
	}
}

func (qr *QueryResponse) SetTransactionId(transactionId []byte) {
	qr.transactionId = transactionId
}

func (qr *QueryResponse) SetWALSequence(sequence int64) {
	qr.walSequence = sequence
}

func (qr *QueryResponse) SetWALTimestamp(timestamp int64) {
	qr.walTimestamp = timestamp
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
			"changes":            qr.changes,
			"id":                 string(qr.id),
			"latency":            qr.latency,
			"last_insert_row_id": qr.lastInsertRowId,
			"columns":            qr.columns,
			"rows":               qr.rows,
			"row_count":          qr.rowCount,
			"transaction_id":     string(qr.transactionId),
		},
	}
}

func (qr *QueryResponse) TransactionId() []byte {
	return qr.transactionId
}

func (qr *QueryResponse) WALSequence() int64 {
	return qr.walSequence
}

func (qr *QueryResponse) WALTimestamp() int64 {
	return qr.walTimestamp
}

func (qr *QueryResponse) WriteJson(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", " ")

	return encoder.Encode(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}
