package sqlite3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
)

/*
A Column is a data structure that represents a column in a database table.

The binary representation of a Column is:
| Offset | Length | Name         | Description                      |
|--------|--------|--------------|----------------------------------|
| 0      | 4      | Type         | The type of the column value.    |
| 4      | 4      | Length       | The length of the column value.  |
| 8     | n      | Value        | The value of the column.         ||
*/

// Buffer pool for reusing buffers
var columnJsonBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type ColumnType int

const (
	ColumnTypeInteger ColumnType = SQLITE_INTEGER
	ColumnTypeFloat   ColumnType = SQLITE_FLOAT
	ColumnTypeText    ColumnType = SQLITE_TEXT
	ColumnTypeBlob    ColumnType = SQLITE_BLOB
	ColumnTypeNull    ColumnType = SQLITE_NULL
)

var ErrInvalidColumnValue = errors.New("invalid column value")

type Column struct {
	ColumnType  ColumnType
	ColumnValue interface{}
}

func NewColumn(columnType ColumnType, columnValue interface{}) Column {
	return Column{
		ColumnType:  columnType,
		ColumnValue: columnValue,
	}
}

func (r Column) Blob() ([]byte, bool) {
	b, ok := r.ColumnValue.([]byte)

	return b, ok
}

func (r Column) Encode(buffer *bytes.Buffer) ([]byte, error) {
	buffer.Reset()

	switch r.ColumnType {
	case ColumnTypeInteger:
		value, ok := r.Int()

		if !ok {
			return nil, ErrInvalidColumnValue
		}

		// Column type
		err := binary.Write(buffer, binary.LittleEndian, uint8(ColumnTypeInteger))

		if err != nil {
			return nil, err
		}

		// Length of the column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(4))

		if err != nil {
			return nil, err
		}

		// Column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(value))

		if err != nil {
			return nil, err
		}

	case ColumnTypeFloat:
		value, ok := r.Float64()

		if !ok {
			return nil, ErrInvalidColumnValue
		}

		// Column type
		err := binary.Write(buffer, binary.LittleEndian, uint8(ColumnTypeFloat))

		if err != nil {
			return nil, err
		}
		// Length of the column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(8))

		if err != nil {
			return nil, err
		}

		// Column value
		err = binary.Write(buffer, binary.LittleEndian, value)

		if err != nil {
			return nil, err
		}
	case ColumnTypeText:

		value, ok := r.String()

		if !ok {
			return nil, ErrInvalidColumnValue
		}

		// Column type
		err := binary.Write(buffer, binary.LittleEndian, uint8(ColumnTypeText))

		if err != nil {
			return nil, err
		}

		// Length of the column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(len(value)))

		if err != nil {
			return nil, err
		}

		// Column value
		err = binary.Write(buffer, binary.LittleEndian, []byte(value))

		if err != nil {
			return nil, err
		}

	case ColumnTypeBlob:
		value, ok := r.Blob()

		if !ok {
			return nil, ErrInvalidColumnValue
		}

		// Column type
		err := binary.Write(buffer, binary.LittleEndian, uint8(ColumnTypeBlob))

		if err != nil {
			return nil, err
		}

		// Length of the column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(len(value)))

		if err != nil {
			return nil, err
		}

		// Column value
		err = binary.Write(buffer, binary.LittleEndian, value)

		if err != nil {
			return nil, err
		}

	case ColumnTypeNull:
		// Column type
		err := binary.Write(buffer, binary.LittleEndian, uint8(ColumnTypeNull))

		if err != nil {
			return nil, err
		}

		// Length of the column value
		err = binary.Write(buffer, binary.LittleEndian, uint32(0))

		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidColumnValue
	}

	return buffer.Bytes(), nil
}

func (r Column) Bool() (bool, bool) {
	b, ok := r.ColumnValue.(bool)

	return b, ok
}

func (r Column) Float64() (float64, bool) {
	f, ok := r.ColumnValue.(float64)

	return f, ok
}

func (r Column) Int() (int, bool) {
	i, ok := r.ColumnValue.(int)

	return i, ok
}

func (r Column) Int64() (int64, bool) {
	i, ok := r.ColumnValue.(int64)

	return i, ok
}

func (r Column) String() (string, bool) {
	str, ok := r.ColumnValue.(string)

	return str, ok
}

// Implement the json.Marshaler interface
func (r Column) MarshalJSON() ([]byte, error) {
	buffer := columnJsonBufferPool.Get().(*bytes.Buffer)
	defer columnJsonBufferPool.Put(buffer)
	buffer.Reset()

	encoder := json.NewEncoder(buffer)

	if err := encoder.Encode(r.ColumnValue); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
