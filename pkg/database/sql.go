package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/litebase/litebase/pkg/sqlite3"
)

var (
	ErrNoRows              = errors.New("sql: no rows in result set")
	ErrRowScanTypeMismatch = errors.New("sql: cannot scan column into destination")
)

// SQLDriver provides a high-level interface for executing SQL queries and scanning results
type SQLDriver struct {
	connection *DatabaseConnection
}

// NewSQLDriver creates a new SQL driver instance using the provided database connection
func NewSQLDriver(connection *DatabaseConnection) *SQLDriver {
	return &SQLDriver{
		connection: connection,
	}
}

// Rows represents a result set from a query
type Rows struct {
	result      *sqlite3.Result
	currentRow  int
	columnNames []string
	closed      bool
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	r.closed = true
	return nil
}

// Next advances to the next row. Returns false when there are no more rows or an error occurs
func (r *Rows) Next() bool {
	if r.closed {
		return false
	}

	if r.currentRow >= len(r.result.Rows)-1 {
		return false
	}

	r.currentRow++
	return true
}

// Scan copies the columns in the current row into the values pointed to by dest
func (r *Rows) Scan(dest ...interface{}) error {
	if r.closed {
		return errors.New("sql: rows are closed")
	}

	if r.currentRow < 0 || r.currentRow >= len(r.result.Rows) {
		return ErrNoRows
	}

	row := r.result.Rows[r.currentRow]

	if len(dest) != len(row) {
		return fmt.Errorf("sql: expected %d destination arguments, got %d", len(row), len(dest))
	}

	for i, col := range row {
		if err := scanColumn(col, dest[i]); err != nil {
			return fmt.Errorf("sql: scan column %d: %w", i, err)
		}
	}

	return nil
}

// Columns returns the column names
func (r *Rows) Columns() ([]string, error) {
	return r.columnNames, nil
}

// Exec executes a query without returning rows
func (s *SQLDriver) Exec(query string, args ...interface{}) (*sqlite3.Result, error) {
	parameters, err := convertArgs(args)
	if err != nil {
		return nil, err
	}

	return s.connection.Exec(query, parameters)
}

// Query executes a query that returns rows
func (s *SQLDriver) Query(query string, args ...interface{}) (*Rows, error) {
	parameters, err := convertArgs(args)
	if err != nil {
		return nil, err
	}

	result, err := s.connection.Exec(query, parameters)
	if err != nil {
		return nil, err
	}

	return &Rows{
		result:      result,
		currentRow:  -1,
		columnNames: result.Columns,
		closed:      false,
	}, nil
}

// QueryRow executes a query that is expected to return at most one row
func (s *SQLDriver) QueryRow(query string, args ...interface{}) *SQLRow {
	rows, err := s.Query(query, args...)
	return &SQLRow{rows: rows, err: err}
}

// SQLRow represents a single row result
type SQLRow struct {
	rows *Rows
	err  error
}

// Scan scans the row into the provided destinations
func (r *SQLRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()

	if !r.rows.Next() {
		return ErrNoRows
	}

	return r.rows.Scan(dest...)
}

// ScanStruct scans a single row into a struct
func (s *SQLDriver) ScanStruct(query string, dest interface{}, args ...interface{}) error {
	row := s.QueryRow(query, args...)
	return scanRowIntoStruct(row, dest)
}

// ScanStructs scans multiple rows into a slice of structs
func (s *SQLDriver) ScanStructs(query string, dest interface{}, args ...interface{}) error {
	rows, err := s.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanRowsIntoStructs(rows, dest)
}

// Helper function to scan a single row into a struct
func scanRowIntoStruct(row *SQLRow, dest interface{}) error {
	if row.err != nil {
		return row.err
	}

	defer row.rows.Close()

	if !row.rows.Next() {
		return ErrNoRows
	}

	return scanStructFromRow(row.rows, dest)
}

// Helper function to scan multiple rows into a slice of structs
func scanRowsIntoStructs(rows *Rows, dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return errors.New("sql: destination must be a pointer to slice")
	}

	destValue = destValue.Elem()
	if destValue.Kind() != reflect.Slice {
		return errors.New("sql: destination must be a pointer to slice")
	}

	elemType := destValue.Type().Elem()

	// Handle both slices of structs and slices of pointers to structs
	var isPointerSlice bool
	var structType reflect.Type

	if elemType.Kind() == reflect.Ptr {
		isPointerSlice = true
		structType = elemType.Elem()
		if structType.Kind() != reflect.Struct {
			return errors.New("sql: slice element must be a struct or pointer to struct")
		}
	} else if elemType.Kind() == reflect.Struct {
		isPointerSlice = false
		structType = elemType
	} else {
		return errors.New("sql: slice element must be a struct or pointer to struct")
	}

	// Create a new slice
	newSlice := reflect.MakeSlice(destValue.Type(), 0, 0)

	for rows.Next() {
		// Create a new element of the appropriate type
		var elem reflect.Value
		var scanTarget interface{}

		if isPointerSlice {
			// For pointer slices, create a new struct instance
			structInstance := reflect.New(structType)
			elem = structInstance
			scanTarget = structInstance.Interface()
		} else {
			// For value slices, create a new struct value
			elem = reflect.New(structType).Elem()
			scanTarget = elem.Addr().Interface()
		}

		// Scan the row into the element
		if err := scanStructFromRow(rows, scanTarget); err != nil {
			return err
		}

		// Append to slice
		newSlice = reflect.Append(newSlice, elem)
	}

	// Set the destination slice to our new slice
	destValue.Set(newSlice)
	return nil
}

// Helper function to scan a row into a struct using reflection
func scanStructFromRow(rows *Rows, dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return errors.New("sql: destination must be a pointer to struct")
	}

	destValue = destValue.Elem()
	if destValue.Kind() != reflect.Struct {
		return errors.New("sql: destination must be a pointer to struct")
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Create a map of column names to field indices
	fieldMap := make(map[string]int)
	destType := destValue.Type()

	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)

		// Check for json tag first, then use field name
		var columnName string
		if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			columnName = strings.Split(jsonTag, ",")[0]
		} else if dbTag := field.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			columnName = strings.Split(dbTag, ",")[0]
		} else {
			columnName = camelToSnake(field.Name)
		}

		fieldMap[columnName] = i
	}

	// Create destination slice for Scan
	dests := make([]interface{}, len(columns))
	fieldPtrs := make([]*reflect.Value, len(columns))

	for i, columnName := range columns {
		if fieldIndex, exists := fieldMap[columnName]; exists {
			field := destValue.Field(fieldIndex)
			if field.CanSet() {
				fieldPtrs[i] = &field
				dests[i] = field.Addr().Interface()
			} else {
				// Field can't be set, use a dummy destination
				var dummy interface{}
				dests[i] = &dummy
			}
		} else {
			// Column doesn't match any field, use a dummy destination
			var dummy interface{}
			dests[i] = &dummy
		}
	}

	return rows.Scan(dests...)
}

// Helper function to convert camelCase to snake_case
func camelToSnake(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r - 'A' + 'a')
	}
	return result.String()
}

// Helper function to scan a column value into a destination
func scanColumn(col *sqlite3.Column, dest interface{}) error {
	if col == nil {
		return nil
	}

	switch col.ColumnType {
	case sqlite3.ColumnTypeNull:
		return setNull(dest)
	case sqlite3.ColumnTypeInteger:
		return setInt64(dest, col.Int64())
	case sqlite3.ColumnTypeFloat:
		return setFloat64(dest, col.Float64())
	case sqlite3.ColumnTypeText:
		return setString(dest, string(col.Text()))
	case sqlite3.ColumnTypeBlob:
		return setBytes(dest, col.Blob())
	default:
		return ErrRowScanTypeMismatch
	}
}

// Helper functions to set values based on destination type
func setNull(dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return ErrRowScanTypeMismatch
	}

	destValue = destValue.Elem()
	destValue.Set(reflect.Zero(destValue.Type()))
	return nil
}

func setInt64(dest interface{}, value int64) error {
	switch d := dest.(type) {
	case *int64:
		*d = value
	case *int:
		*d = int(value)
	case *int32:
		*d = int32(value)
	case *int16:
		*d = int16(value)
	case *int8:
		*d = int8(value)
	case *uint64:
		*d = uint64(value)
	case *uint:
		*d = uint(value)
	case *uint32:
		*d = uint32(value)
	case *uint16:
		*d = uint16(value)
	case *uint8:
		*d = uint8(value)
	case *string:
		*d = strconv.FormatInt(value, 10)
	case *interface{}:
		*d = value
	default:
		return ErrRowScanTypeMismatch
	}
	return nil
}

func setFloat64(dest interface{}, value float64) error {
	switch d := dest.(type) {
	case *float64:
		*d = value
	case *float32:
		*d = float32(value)
	case *string:
		*d = strconv.FormatFloat(value, 'f', -1, 64)
	case *interface{}:
		*d = value
	default:
		return ErrRowScanTypeMismatch
	}
	return nil
}

func setString(dest interface{}, value string) error {
	switch d := dest.(type) {
	case *string:
		*d = value
	case *[]byte:
		*d = []byte(value)
	case *time.Time:
		// Try to parse common time formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02",
		}

		var parsed time.Time
		var err error
		for _, format := range formats {
			parsed, err = time.Parse(format, value)
			if err == nil {
				*d = parsed
				return nil
			}
		}
		return fmt.Errorf("sql: cannot parse time string %q", value)
	case *interface{}:
		*d = value
	default:
		// Try JSON unmarshaling for struct types
		destValue := reflect.ValueOf(dest)
		if destValue.Kind() == reflect.Ptr && destValue.Elem().Kind() == reflect.Struct {
			return json.Unmarshal([]byte(value), dest)
		}
		return ErrRowScanTypeMismatch
	}
	return nil
}

func setBytes(dest interface{}, value []byte) error {
	switch d := dest.(type) {
	case *[]byte:
		*d = value
	case *string:
		*d = string(value)
	case *interface{}:
		*d = value
	default:
		return ErrRowScanTypeMismatch
	}
	return nil
}

// Helper function to convert arguments to StatementParameters
func convertArgs(args []interface{}) ([]sqlite3.StatementParameter, error) {
	params := make([]sqlite3.StatementParameter, len(args))

	for i, arg := range args {
		param, err := convertToParameter(arg)
		if err != nil {
			return nil, fmt.Errorf("sql: convert argument %d: %w", i, err)
		}
		params[i] = param
	}

	return params, nil
}

// Helper function to convert a single argument to StatementParameter
func convertToParameter(arg interface{}) (sqlite3.StatementParameter, error) {
	if arg == nil {
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeNull,
			Value: nil,
		}, nil
	}

	switch v := arg.(type) {
	case int, int8, int16, int32, int64:
		// Convert all integer types to int64
		val := reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Int()
		bytes := make([]byte, 8)
		for i := 0; i < 8; i++ {
			bytes[i] = byte(val >> (i * 8))
		}
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeInteger,
			Value: bytes,
		}, nil
	case uint, uint8, uint16, uint32, uint64:
		// Convert all unsigned integer types to int64
		val := int64(reflect.ValueOf(v).Convert(reflect.TypeOf(uint64(0))).Uint())
		bytes := make([]byte, 8)
		for i := 0; i < 8; i++ {
			bytes[i] = byte(val >> (i * 8))
		}
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeInteger,
			Value: bytes,
		}, nil
	case float32, float64:
		val := reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Float()
		bytes := make([]byte, 8)
		bits := *(*uint64)(unsafe.Pointer(&val))
		for i := 0; i < 8; i++ {
			bytes[i] = byte(bits >> (i * 8))
		}
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeFloat,
			Value: bytes,
		}, nil
	case string:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte(v),
		}, nil
	case []byte:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeBlob,
			Value: v,
		}, nil
	case time.Time:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte(v.Format(time.RFC3339)),
		}, nil
	default:
		return sqlite3.StatementParameter{}, fmt.Errorf("sql: unsupported argument type %T", arg)
	}
}
