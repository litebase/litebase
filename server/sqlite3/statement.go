package sqlite3

/*
#include <stdlib.h>
#include <stdio.h>
#include "./sqlite3.h"
*/
import "C"
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"unsafe"
)

var statementBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

type Statement struct {
	columnCount  int
	columnTypes  []ColumnType
	columnNames  []string
	columns      []Column
	Connection   *Connection
	context      context.Context
	isReadOnly   StatementReadonly
	sqlite3_stmt *C.sqlite3_stmt
	extra        *C.char
	text         []byte
}

type StatementReadonly string

const (
	StatementReadonlyTrue  StatementReadonly = "true"
	StatementReadonlyFalse StatementReadonly = "false"
)

func NewStatement(
	ctx context.Context,
	c *Connection,
	query []byte,
) (*Statement, int, error) {
	var cQuery, cExtra *C.char
	var s *C.sqlite3_stmt

	cQuery = C.CString(string(query))
	defer C.free(unsafe.Pointer(cQuery))

	if err := C.sqlite3_prepare_v3((*C.sqlite3)(c.sqlite3), cQuery, -1, C.SQLITE_PREPARE_PERSISTENT, &s, &cExtra); err != SQLITE_OK {
		return nil, int(err), c.Error(int(err))
	}

	return &Statement{
		columnCount:  0,
		columnNames:  []string{},
		Connection:   c,
		context:      ctx,
		sqlite3_stmt: s,
		extra:        cExtra,
		text:         query,
	}, 0, nil
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
			value, ok := parameter.Value.(int64)

			if !ok {
				return errors.New("parameter value is not an integer")
			}

			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case "FLOAT":
		case "REAL":
			value, ok := parameter.Value.(float64)

			if !ok {
				return errors.New("parameter value is not a real")
			}

			rc = C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(value))
		case "NULL":
			rc = C.sqlite3_bind_null(s.sqlite3_stmt, index)
		case "TEXT":
			value := parameter.Value.([]byte)

			cText := (*C.char)(unsafe.Pointer(&value[0]))

			cTextLen := C.int(len(value))

			rc = C.sqlite3_bind_text(s.sqlite3_stmt, C.int(index), cText, cTextLen, C.SQLITE_TRANSIENT)
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

// Clear the bindings of the statement
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

// Get the column count of the statement
func (s *Statement) ColumnCount() int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	if s.columnCount == 0 {
		s.columnCount = int(C.sqlite3_column_count(s.sqlite3_stmt))
	}

	return s.columnCount
}

// Get the name of a column by index
func (s *Statement) ColumnName(index int) string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_column_name(s.sqlite3_stmt, C.int(index)))
}

// Get the names of all columns
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

// Get the value of a column by index
func (s *Statement) ColumnValue(buffer *bytes.Buffer, columnType ColumnType, index int) []byte {
	if s.sqlite3_stmt == nil {
		return nil
	}

	switch columnType {
	case SQLITE_INTEGER:
		var columnValueBytes [8]byte
		binary.LittleEndian.PutUint64(columnValueBytes[:], uint64(int64(C.sqlite3_column_int64(s.sqlite3_stmt, C.int(index)))))
		buffer.Write(columnValueBytes[:])

		return buffer.Bytes()
	case SQLITE_FLOAT:
		var columnValueBytes [8]byte
		binary.LittleEndian.PutUint64(columnValueBytes[:], math.Float64bits(float64(C.sqlite3_column_double(s.sqlite3_stmt, C.int(index)))))
		buffer.Write(columnValueBytes[:])

		return buffer.Bytes()
	case SQLITE_TEXT:
		buffer.Write(s.getTextData(buffer, index))

		return buffer.Bytes()
	case SQLITE_BLOB:
		buffer.Write(s.getBlobData(index))

		return buffer.Bytes()
	case SQLITE_NULL:
		return nil
	default:
		return nil
	}
}

// Bind the parameteres to the statement and return the results
func (s *Statement) Exec(result *Result, parameters ...StatementParameter) error {
	defer s.Reset()

	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	if len(parameters) > 0 {
		if err := s.Bind(parameters...); err != nil {
			return err
		}
	}

	if result != nil &&
		!bytes.Equal(s.text, []byte("COMMIT")) &&
		!bytes.Equal(s.text, []byte("ROLLBACK")) {
		result.Reset()
		result.SetColumns(s.ColumnNames())
	}

	rowIndex := -1

	for {
		select {
		case <-s.context.Done():
			return errors.New("context done")
		default:
			rc := s.Step()

			if rc == SQLITE_DONE {
				return nil
			} else if rc == SQLITE_BUSY {
				continue
			} else if rc == SQLITE_ROW {
				rowIndex++

				// Set the column types slice to the length of the result columns
				if len(s.columnTypes) == 0 {
					s.setColumnTypes(result)
				}

				if result == nil {
					return errors.New("result is nil")
				}

				if len(result.Columns) >= 0 {
					for rowIndex >= len(result.Rows) {
						result.Rows = append(result.Rows, make([]*Column, len(result.Columns)))

						// Initialize the columns slice if there are no existing rows
						for i := range result.Columns {
							result.Rows[rowIndex][i] = result.GetColumn()
						}
					}

					for i := range result.Columns {
						result.Rows[rowIndex][i].ColumnType = s.columnTypes[i]
						result.Rows[rowIndex][i].ColumnValue = s.ColumnValue(
							result.GetBuffer(),
							s.columnTypes[i],
							i,
						)
					}
				}
			} else {
				return s.Connection.Error(rc)
			}
		}
	}
}

// Finalize the statement
// https://www.sqlite.org/c3ref/finalize.html
func (s *Statement) Finalize() error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	rc := C.sqlite3_finalize(s.sqlite3_stmt)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	s.sqlite3_stmt = nil

	return nil
}

// Get the blob data
func (s *Statement) getBlobData(index int) []byte {
	buf := statementBufferPool.Get().(*bytes.Buffer)
	defer statementBufferPool.Put(buf)

	buf.Reset()

	// Get the size of the blob data
	size := int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index)))

	// Ensure the buffer is large enough
	if buf.Cap() < size {
		buf.Grow(size)
	}

	// Get the pointer to the blob data
	blobPtr := C.sqlite3_column_blob(s.sqlite3_stmt, C.int(index))

	if blobPtr == nil {
		return nil
	}

	// Copy the blob data into the buffer
	byteSlice := buf.Bytes()
	copy(byteSlice, (*[1 << 30]byte)(unsafe.Pointer(blobPtr))[:size:size])

	// Return a slice of the buffer containing the blob data

	return byteSlice
}

// Use the text buffer to store the text data
func (s *Statement) getTextData(buf *bytes.Buffer, index int) []byte {
	buf.Reset()

	// Get the size of the text data
	size := int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index)))

	// Ensure the buffer is large enough
	if buf.Cap() < size {
		buf.Grow(size)
	}

	// Get the pointer to the text data
	textPtr := C.sqlite3_column_text(s.sqlite3_stmt, C.int(index))

	if textPtr == nil {
		return []byte{}
	}

	byteSlice := buf.Bytes()[0:size]

	// Copy the text data into the buffer
	copy(byteSlice, (*[1 << 30]byte)(unsafe.Pointer(textPtr))[:size:size])

	// Return a slice of the buffer containing the text data
	return byteSlice
}

// Determine if the statement is read-only
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

// Get the number of parameters in the statement
func (s *Statement) ParameterCount() int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	return int(C.sqlite3_bind_parameter_count(s.sqlite3_stmt))
}

// Get the index of a parameter by name
func (s *Statement) ParameterIndex(parameter string) int {
	if s.sqlite3_stmt == nil {
		return 0
	}

	cString := C.CString(parameter)
	defer C.free(unsafe.Pointer(cString))

	return int(C.sqlite3_bind_parameter_index(s.sqlite3_stmt, cString))
}

// Get the name of a parameter by index
func (s *Statement) ParameterName(index int) string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_bind_parameter_name(s.sqlite3_stmt, C.int(index)))
}

// Reset the statement
func (s *Statement) Reset() error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	err := C.sqlite3_reset(s.sqlite3_stmt)

	if err != SQLITE_OK {
		return s.Connection.Error(int(err))
	} else {
		return nil
	}
}

// Return the SQL of the satement
func (s *Statement) SQL() string {
	if s.sqlite3_stmt == nil {
		return ""
	}

	return C.GoString(C.sqlite3_sql(s.sqlite3_stmt))
}

// Set the column types of the statement result
func (s *Statement) setColumnTypes(result *Result) {
	for i := range result.Columns {
		if i >= len(s.columnTypes) {
			// Expand the columnTypes slice to accommodate the new index
			newColumnTypes := make([]ColumnType, i+1)
			copy(newColumnTypes, s.columnTypes)
			s.columnTypes = newColumnTypes
		}

		if s.columnTypes[i] == 0 {
			// Get the column type and cache it
			s.columnTypes[i] = ColumnType(C.sqlite3_column_type(s.sqlite3_stmt, C.int(i)))
		}
	}
}

// Step the statement
func (s *Statement) Step() int {
	if s.sqlite3_stmt == nil {
		return int(SQLITE_ERROR)
	}

	return int(C.sqlite3_step(s.sqlite3_stmt))
}
