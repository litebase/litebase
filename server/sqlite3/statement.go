package sqlite3

/*
 #include <stdlib.h>
 #include <stdio.h>
 #include "./sqlite3.h"
*/
import "C"
import (
	"context"
	"errors"
	"sync"
	"unsafe"
)

var statementBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 1024)

		return &buf
	},
}

type Statement struct {
	columnCount  int
	columnNames  []string
	Connection   *Connection
	context      context.Context
	isReadOnly   StatementReadonly
	sqlite3_stmt *C.sqlite3_stmt
	extra        *C.char
	text         string
}

type StatementReadonly string

const (
	StatementReadonlyTrue  StatementReadonly = "true"
	StatementReadonlyFalse StatementReadonly = "false"
)

// Prepare query
func (c *Connection) Prepare(ctx context.Context, query string) (*Statement, error) {
	var cQuery, cExtra *C.char
	var s *C.sqlite3_stmt

	cQuery = C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if err := C.sqlite3_prepare_v3((*C.sqlite3)(c.sqlite3), cQuery, -1, C.SQLITE_PREPARE_PERSISTENT, &s, &cExtra); err != SQLITE_OK {
		return nil, c.Error(int(err))
	}

	// Return prepared statement and extra string
	return &Statement{
		columnCount:  0,
		columnNames:  []string{},
		Connection:   c,
		context:      ctx,
		sqlite3_stmt: s,
		extra:        cExtra,
		text:         query,
	}, nil
}

func (s *Statement) Reset() error {
	err := C.sqlite3_reset(s.sqlite3_stmt)

	if err != SQLITE_OK {
		return s.Connection.Error(int(err))
	} else {
		return nil
	}
}

// Bind parameters to statement
func (s *Statement) Bind(parameters ...StatementParameter) error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	for i, parameter := range parameters {
		index := C.int(i + 1)

		var rc C.int
		switch parameter.Type {
		case "INTEGER":
			value, ok := parameter.Value.(float64)

			if !ok {
				return errors.New("parameter value is not an integer")
			}

			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case "REAL":
			value, ok := parameter.Value.(float64)

			if !ok {
				return errors.New("parameter value is not a real")
			}

			rc = C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(value))
		case "NULL":
			rc = C.sqlite3_bind_null(s.sqlite3_stmt, index)
		case "TEXT":
			buf := getStatementBuffer()
			value := parameter.Value.(string)

			// Ensure the buffer is large enough
			if cap(*buf) < len(value)+1 {
				*buf = make([]byte, len(value)+1)
			} else {
				*buf = (*buf)[:len(value)+1]
			}

			// Copy the string into the buffer
			copy(*buf, value)
			(*buf)[len(value)] = 0

			// Convert the buffer to a CString
			cText := (*C.char)(unsafe.Pointer(&(*buf)[0]))
			cTextLen := C.int(len(value))

			rc = C.sqlite3_bind_text(s.sqlite3_stmt, C.int(index), cText, cTextLen, C.SQLITE_TRANSIENT)

			// Return the buffer to the pool
			putStatementBuffer(buf)
		case "BLOB":
			var valuePointer unsafe.Pointer
			value := parameter.Value.([]byte)

			if len(value) > 0 {
				valuePointer = unsafe.Pointer(&value[0])
			}

			rc = C.sqlite3_bind_blob(s.sqlite3_stmt, index, valuePointer, C.int(len(value)), C.SQLITE_TRANSIENT)
		default:
			rc = C.sqlite3_bind_null(s.sqlite3_stmt, index)
		}

		if rc != SQLITE_OK {
			return s.Connection.Error(int(rc))
		}
	}

	return nil
}

// Bind the parameteres to the statement and return the results
func (s *Statement) Exec(parameters ...StatementParameter) (Result, error) {
	defer s.Reset()

	if s.sqlite3_stmt == nil {
		return Result{}, errors.New("sqlite3 statement is nil")
	}

	if len(parameters) > 0 {
		if err := s.Bind(parameters...); err != nil {
			return Result{}, err
		}
	}

	result := Result{
		Columns: []string{},
		Rows:    [][]Column{},
	}

	if s.text != "COMMIT" && s.text != "ROLLBACK" {
		result.Columns = s.ColumnNames()
	}

	for {
		select {
		case <-s.context.Done():
			return Result{}, errors.New("context done")
		default:
			rc := s.Step()

			if rc == SQLITE_DONE {
				return result, nil
			} else if rc == SQLITE_BUSY {
				continue
			} else if rc == SQLITE_ROW {
				if len(result.Columns) > 0 {
					columns := make([]Column, len(result.Columns))

					for i := range result.Columns {
						columnType := C.sqlite3_column_type(s.sqlite3_stmt, C.int(i))
						columnTypeInt := ColumnType(columnType)
						columns[i] = NewColumn(columnTypeInt, s.ColumnValue(columnTypeInt, i))
					}

					result.Rows = append(result.Rows, columns)
				}
			} else {
				return Result{}, s.Connection.Error(rc)
			}
		}
	}
}

// Bind the parameteres to the statement and return the results
func (s *Statement) ExecStreaming(parameters ...StatementParameter) (Result, chan [][]Column, error) {
	defer s.Reset()

	if s.sqlite3_stmt == nil {
		return Result{}, nil, errors.New("sqlite3 statement is nil")
	}

	if len(parameters) > 0 {
		if err := s.Bind(parameters...); err != nil {
			return Result{}, nil, err
		}
	}

	result := Result{}

	if s.text != "COMMIT" && s.text != "ROLLBACK" {
		result.Columns = s.ColumnNames()
	}

	for {
		select {
		case <-s.context.Done():
			return Result{}, nil, errors.New("context done")
		default:
			rc := s.Step()

			if rc == SQLITE_DONE {
				return result, nil, nil
			} else if rc == SQLITE_BUSY {
				continue
			} else if rc == SQLITE_ROW {
				columns := make([]Column, len(result.Columns))

				for i := range result.Columns {
					columnType := C.sqlite3_column_type(s.sqlite3_stmt, C.int(i))
					columnTypeInt := ColumnType(columnType)
					columns[i] = NewColumn(columnTypeInt, s.ColumnValue(columnTypeInt, i))
				}

				result.Rows = append(result.Rows, columns)
			} else {
				return Result{}, nil, s.Connection.Error(rc)
			}
		}
	}
}

// https://www.sqlite.org/c3ref/finalize.html
func (s *Statement) Finalize() error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	rc := C.sqlite3_finalize(s.sqlite3_stmt)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (s *Statement) getBlobData(index int) []byte {
	buf := getStatementBuffer()

	// Get the size of the blob data
	size := int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index)))

	// Ensure the buffer is large enough
	if cap(*buf) < size {
		*buf = make([]byte, size)
	} else {
		*buf = (*buf)[:size]
	}

	// Get the pointer to the blob data
	blobPtr := C.sqlite3_column_blob(s.sqlite3_stmt, C.int(index))

	if blobPtr == nil {
		return nil
	}

	// Copy the blob data into the buffer
	copy(*buf, (*[1 << 30]byte)(unsafe.Pointer(blobPtr))[:size:size])

	defer putStatementBuffer(buf)

	// Return a slice of the buffer containing the blob data
	return *buf
}

// Use the text buffer to store the text data
func (s *Statement) getTextData(index int) string {
	buf := getStatementBuffer()

	// Get the size of the text data
	size := int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index)))

	// Ensure the buffer is large enough
	if cap(*buf) < size {
		*buf = make([]byte, size)
	} else {
		*buf = (*buf)[:size]
	}

	// Get the pointer to the text data
	textPtr := C.sqlite3_column_text(s.sqlite3_stmt, C.int(index))

	if textPtr == nil {
		return ""
	}

	// Copy the text data into the buffer
	copy(*buf, (*[1 << 30]byte)(unsafe.Pointer(textPtr))[:size:size])

	defer putStatementBuffer(buf)

	// Return a slice of the buffer containing the text data
	return string(*buf)
}

func (s *Statement) IsBusy() bool {
	if s.sqlite3_stmt == nil {
		return false
	}

	result := int(C.sqlite3_stmt_busy(s.sqlite3_stmt))

	return result != 0
}

func (s *Statement) ColumnCount() int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	if s.columnCount == 0 {
		s.columnCount = int(C.sqlite3_column_count(s.sqlite3_stmt))
	}

	return s.columnCount
}

func (s *Statement) ColumnName(index int) string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_column_name(s.sqlite3_stmt, C.int(index)))
}

func (s *Statement) ColumnNames() []string {
	if s.sqlite3_stmt == nil {
		return []string{}
	}

	if len(s.columnNames) == 0 {
		columnCount := s.ColumnCount()
		s.columnNames = make([]string, 0, columnCount)

		for i := 0; i < columnCount; i++ {
			s.columnNames = append(s.columnNames, s.ColumnName(i))
		}
	}

	return s.columnNames
}

func (s *Statement) ColumnValue(columnType ColumnType, index int) any {
	if s.sqlite3_stmt == nil {
		return nil
	}

	switch columnType {
	case SQLITE_INTEGER:
		return int64(C.sqlite3_column_int64(s.sqlite3_stmt, C.int(index)))
	case SQLITE_FLOAT:
		return float64(C.sqlite3_column_double(s.sqlite3_stmt, C.int(index)))
	case SQLITE_TEXT:
		// return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.sqlite3_stmt, C.int(index)))))
		return s.getTextData(index)
	case SQLITE_BLOB:
		return s.getBlobData(index)
	case SQLITE_NULL:
		return nil
	default:
		return nil
	}
}

func (s *Statement) ClearBindings() error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	if rc := C.sqlite3_clear_bindings(s.sqlite3_stmt); rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	} else {
		return nil
	}
}

func (s *Statement) IsReadonly() bool {
	if s.sqlite3_stmt == nil {
		return false
	}

	if s.isReadOnly != "" {
		return s.isReadOnly == StatementReadonlyTrue
	}

	readonly := int(C.sqlite3_stmt_readonly((*C.sqlite3_stmt)(s.sqlite3_stmt))) != 0

	if readonly {
		s.isReadOnly = StatementReadonlyTrue
	} else {
		s.isReadOnly = StatementReadonlyFalse
	}

	return readonly
}

func (s *Statement) SQL() string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_sql(s.sqlite3_stmt))
}

func (s *Statement) ParameterCount() int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	return int(C.sqlite3_bind_parameter_count(s.sqlite3_stmt))
}

func (s *Statement) ParameterIndex(parameter string) int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	cString := C.CString(parameter)
	defer C.free(unsafe.Pointer(cString))

	return int(C.sqlite3_bind_parameter_index(s.sqlite3_stmt, cString))
}

func (s *Statement) ParameterName(index int) string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_bind_parameter_name(s.sqlite3_stmt, C.int(index)))
}

func (s *Statement) Step() int {
	return int(C.sqlite3_step(s.sqlite3_stmt))
}

// Function to get a buffer from the pool
func getStatementBuffer() *[]byte {
	return statementBufferPool.Get().(*[]byte)
}

// Function to return a buffer to the pool
func putStatementBuffer(buf *[]byte) {
	// Reset the buffer before putting it back to the pool
	*buf = (*buf)[:0] // Reset the buffer before putting it back to the pool

	statementBufferPool.Put(buf)
}
