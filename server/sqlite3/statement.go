package sqlite3

// #include <stdlib.h>
// #include <stdio.h>
// #include "./sqlite3.h"
import "C"
import (
	"errors"
	"unsafe"
)

type Statement struct {
	columnCount  int
	columnNames  []string
	Connection   *Connection
	sqlite3_stmt *C.sqlite3_stmt
	extra        *C.char
	text         string
}

// Prepare query
func (c *Connection) Prepare(query string) (*Statement, error) {
	var cQuery, cExtra *C.char
	var s *C.sqlite3_stmt

	cQuery = C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if err := C.sqlite3_prepare_v3((*C.sqlite3)(c), cQuery, -1, C.SQLITE_PREPARE_PERSISTENT, &s, &cExtra); err != SQLITE_OK {
		return nil, c.Error(err)
	}

	// Return prepared statement and extra string
	return &Statement{
		columnCount:  0,
		columnNames:  []string{},
		Connection:   c,
		sqlite3_stmt: s,
		extra:        cExtra,
		text:         query,
	}, nil
}

func (s *Statement) Reset() error {
	err := C.sqlite3_reset(s.sqlite3_stmt)

	if err != SQLITE_OK {
		return s.Connection.Error(err)
	} else {
		return nil
	}
}

// Bind parameters to statement
func (s *Statement) Bind(parameters ...interface{}) error {
	if s.sqlite3_stmt == nil {
		return errors.New("sqlite3 statement is nil")
	}

	for i, parameter := range parameters {
		index := C.int(i + 1)

		var rc C.int

		switch value := parameter.(type) {
		case int:
			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case int8:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case int16:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case int32:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case int64:
			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case uint:
			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case uint8:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case uint16:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case uint32:
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(value))
		case uint64:
			rc = C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
		case float32:
			rc = C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(value))
		case float64:
			rc = C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(value))
		case bool:
			var boolean int
			if value {
				boolean = 1
			} else {
				boolean = 0
			}
			rc = C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(boolean))
		case string:
			cText := C.CString(value)
			cTextLen := C.int(len(value))
			defer C.free(unsafe.Pointer(cText))
			rc = C.sqlite3_bind_text(s.sqlite3_stmt, index, cText, cTextLen, C.SQLITE_TRANSIENT)
		case []byte:
			var valuePointer unsafe.Pointer
			if len(value) > 0 {
				valuePointer = unsafe.Pointer(&value[0])
			}
			rc = C.sqlite3_bind_blob(s.sqlite3_stmt, index, valuePointer, C.int(len(value)), C.SQLITE_TRANSIENT)
		default:
			rc = C.sqlite3_bind_null(s.sqlite3_stmt, index)
		}

		if rc != SQLITE_OK {
			return s.Connection.Error(rc)
		}
	}

	return nil
}

// Bind the parameteres to the statement and return the results
func (s *Statement) Exec(parameters ...interface{}) (Result, error) {
	defer s.Reset()

	if s.sqlite3_stmt == nil {
		return nil, errors.New("sqlite3 statement is nil")
	}

	if err := s.Bind(parameters...); err != nil {
		return nil, err
	}

	var results []map[string]any
	columnNames := s.ColumnNames()

	for {
		rc := C.sqlite3_step(s.sqlite3_stmt)

		if rc == SQLITE_DONE {
			break
		} else if rc == SQLITE_BUSY {
			continue
		} else if rc == SQLITE_ROW {
			result := make(map[string]any)

			for i, columnName := range columnNames {
				result[columnName] = s.ColumnValue(i)
			}

			results = append(results, result)
		} else {
			return nil, s.Connection.Error(rc)
		}
	}

	// if err := s.ClearBindings(); err != nil {
	// 	return nil, err
	// }

	return results, nil
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

func (s *Statement) ColumnValue(index int) any {
	if s.sqlite3_stmt == nil {
		return nil
	}

	switch C.sqlite3_column_type(s.sqlite3_stmt, C.int(index)) {
	case SQLITE_INTEGER:
		return int64(C.sqlite3_column_int64(s.sqlite3_stmt, C.int(index)))
	case SQLITE_FLOAT:
		return float64(C.sqlite3_column_double(s.sqlite3_stmt, C.int(index)))
	case SQLITE_TEXT:
		return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.sqlite3_stmt, C.int(index)))))
	case SQLITE_BLOB:
		return C.GoBytes(unsafe.Pointer(C.sqlite3_column_blob(s.sqlite3_stmt, C.int(index))), C.int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index))))
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

	return int(C.sqlite3_stmt_readonly((*C.sqlite3_stmt)(s.sqlite3_stmt))) != 0
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
