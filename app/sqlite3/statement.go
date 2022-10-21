package sqlite3

// #include <stdlib.h>
// #include <stdio.h>
// #include <sqlite3.h>
import "C"
import (
	"errors"
	"unsafe"
)

type Statement struct {
	Connection   *Connection
	sqlite3_stmt *C.sqlite3_stmt
	extra        string
}

func (s *Statement) ParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.sqlite3_stmt))
}

func (s *Statement) ParameterIndex(parameter string) int {
	cString := C.CString(parameter)
	defer C.free(unsafe.Pointer(cString))

	return int(C.sqlite3_bind_parameter_index(s.sqlite3_stmt, cString))
}

func (s *Statement) ParameterName(index int) string {
	return C.GoString(C.sqlite3_bind_parameter_name(s.sqlite3_stmt, C.int(index)))
}

func (s *Statement) Reset() error {
	err := C.sqlite3_reset(s.sqlite3_stmt)

	if err != C.SQLITE_OK {
		return s.Connection.Error(err)
	} else {
		return nil
	}
}

// Bind parameters to statement
func (s *Statement) Bind(parameters ...interface{}) error {
	var err error

	for i, parameter := range parameters {
		if parameter == nil {
			if rc := C.sqlite3_bind_null(s.sqlite3_stmt, C.int(i+1)); rc != C.SQLITE_OK {
				return s.Connection.Error(rc)
			}
		}

		index := C.int(i + 1)

		var binding = func(callback func() C.int) error {
			if rc := callback(); rc != C.SQLITE_OK {
				return s.Connection.Error(rc)
			}

			return nil
		}

		switch value := parameter.(type) {
		case int:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case int8:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case int16:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case int32:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case int64:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case uint:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case uint8:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case uint16:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case uint32:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case uint64:
			err = binding(func() C.int {
				return C.sqlite3_bind_int64(s.sqlite3_stmt, index, C.sqlite3_int64(value))
			})
		case float32:
			err = binding(func() C.int {
				return C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(float32(value)))
			})
		case float64:
			err = binding(func() C.int {
				return C.sqlite3_bind_double(s.sqlite3_stmt, index, C.double(float64(value)))
			})
		case bool:
			var boolean int

			if parameter.(bool) {
				boolean = 1
			} else {
				boolean = 0
			}

			err = binding(func() C.int {
				return C.sqlite3_bind_int(s.sqlite3_stmt, index, C.int(boolean))
			})
		case string:
			err = binding(func() C.int {
				cText := C.CString(value)
				cTextLen := C.int(len(value))
				defer C.free(unsafe.Pointer(cText))

				return C.sqlite3_bind_text(s.sqlite3_stmt, index, cText, cTextLen, C.SQLITE_TRANSIENT)
			})
		case []byte:
			var valuePointer unsafe.Pointer

			if len(value) > 0 {
				valuePointer = unsafe.Pointer(&value[0])
			}

			err = binding(func() C.int {
				return C.sqlite3_bind_blob(s.sqlite3_stmt, index, valuePointer, C.int(len(value)), C.SQLITE_TRANSIENT)
			})
		case nil:
			err = binding(func() C.int {
				return C.sqlite3_bind_null(s.sqlite3_stmt, index)
			})
		default:
			err = binding(func() C.int {
				return C.sqlite3_bind_null(s.sqlite3_stmt, index)
			})
		}
	}

	return err
}

// Bind the parameteres to the statement and return the results
func (s *Statement) Exec(parameters ...interface{}) (Result, error) {
	if err := s.Bind(parameters...); err != nil {
		s.Reset()
		return nil, err
	}

	var results []map[string]any

	for {
		rc := C.sqlite3_step(s.sqlite3_stmt)

		if rc == C.SQLITE_DONE {
			break
		} else if rc == C.SQLITE_ROW {
			result := make(map[string]any)

			for i := 0; i < s.ColumnCount(); i++ {
				result[s.ColumnName(i)] = s.ColumnValue(i)
			}

			results = append(results, result)
		} else {
			return nil, s.Connection.Error(rc)
		}
	}

	if err := s.ClearBindings(); err != nil {
		return nil, err
	}

	s.Reset()

	return results, nil
}

// https://www.sqlite.org/c3ref/finalize.html
func (s *Statement) Finalize() error {
	rc := C.sqlite3_finalize(s.sqlite3_stmt)

	if rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (s *Statement) IsBusy() bool {
	result := int(C.sqlite3_stmt_busy(s.sqlite3_stmt))

	return result != 0
}

func (s *Statement) ColumnCount() int {
	return int(C.sqlite3_column_count(s.sqlite3_stmt))
}

func (s *Statement) ColumnName(index int) string {
	return C.GoString(C.sqlite3_column_name(s.sqlite3_stmt, C.int(index)))
}

func (s *Statement) ColumnValue(index int) any {
	switch C.sqlite3_column_type(s.sqlite3_stmt, C.int(index)) {
	case C.SQLITE_INTEGER:
		return int64(C.sqlite3_column_int64(s.sqlite3_stmt, C.int(index)))
	case C.SQLITE_FLOAT:
		return float64(C.sqlite3_column_double(s.sqlite3_stmt, C.int(index)))
	case C.SQLITE_TEXT:
		return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.sqlite3_stmt, C.int(index)))))
	case C.SQLITE_BLOB:
		return C.GoBytes(unsafe.Pointer(C.sqlite3_column_blob(s.sqlite3_stmt, C.int(index))), C.int(C.sqlite3_column_bytes(s.sqlite3_stmt, C.int(index))))
	case C.SQLITE_NULL:
		return nil
	default:
		return nil
	}
}

func (s *Statement) ClearBindings() error {
	if rc := C.sqlite3_clear_bindings(s.sqlite3_stmt); rc != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	} else {
		return nil
	}
}

func (s *Statement) IsReadonly() bool {
	return int(C.sqlite3_stmt_readonly((*C.sqlite3_stmt)(s.sqlite3_stmt))) != 0
}

func (s *Statement) SQL() string {
	return C.GoString(C.sqlite3_sql(s.sqlite3_stmt))
}
