package sqlite3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
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
	ColumnValue []byte
}

func NewColumn(columnType ColumnType, columnValue []byte) *Column {
	return &Column{
		ColumnType:  columnType,
		ColumnValue: columnValue,
	}
}

func (c *Column) Blob() []byte {
	return c.ColumnValue
}

func (c *Column) Encode(buffer *bytes.Buffer) error {
	buffer.Reset()

	switch c.ColumnType {
	case ColumnTypeInteger:
		// Column type
		err := buffer.WriteByte(uint8(ColumnTypeInteger))

		if err != nil {
			return err
		}

		// Length of the column value
		var columnValueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(columnValueLengthBytes[:], uint32(8))
		buffer.Write(columnValueLengthBytes[:])

		if err != nil {
			return err
		}

		// Column value
		_, err = buffer.Write(c.ColumnValue)

		if err != nil {
			return err
		}

	case ColumnTypeFloat:
		// Column type
		err := buffer.WriteByte(uint8(ColumnTypeFloat))

		if err != nil {
			return err
		}

		// Length of the column value
		var columnValueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(columnValueLengthBytes[:], uint32(len(c.ColumnValue)))
		buffer.Write(columnValueLengthBytes[:])

		if err != nil {
			return err
		}

		// Column value
		_, err = buffer.Write(c.ColumnValue)

		if err != nil {
			return err
		}
	case ColumnTypeText:
		// Column type
		err := buffer.WriteByte(uint8(ColumnTypeText))

		if err != nil {
			return err
		}

		// Length of the column value
		var columnValueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(columnValueLengthBytes[:], uint32(len(c.ColumnValue)))
		buffer.Write(columnValueLengthBytes[:])

		if err != nil {
			return err
		}

		// Column value
		_, err = buffer.Write(c.ColumnValue)

		if err != nil {
			return err
		}
	case ColumnTypeBlob:
		// Column type
		err := buffer.WriteByte(uint8(ColumnTypeBlob))

		if err != nil {
			return err
		}

		// Length of the column value
		var columnValueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(columnValueLengthBytes[:], uint32(len(c.ColumnValue)))
		buffer.Write(columnValueLengthBytes[:])

		if err != nil {
			return err
		}

		// Column value
		_, err = buffer.Write(c.ColumnValue)

		if err != nil {
			return err
		}

	case ColumnTypeNull:
		// Column type
		err := buffer.WriteByte(uint8(ColumnTypeNull))

		if err != nil {
			return err
		}

		// Length of the column value
		var columnValueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(columnValueLengthBytes[:], uint32(0))
		buffer.Write(columnValueLengthBytes[:])

		if err != nil {
			return err
		}
	default:
		return ErrInvalidColumnValue
	}

	return nil
}

func (c *Column) Float64() float64 {
	return float64(math.Float64frombits(binary.LittleEndian.Uint64(c.ColumnValue)))
}

func (c *Column) Int64() int64 {
	return int64(binary.LittleEndian.Uint64(c.ColumnValue))
}

func (c *Column) Reset() {
	c.ColumnType = 0
	c.ColumnValue = nil
}

func (c *Column) Text() []byte {
	return c.ColumnValue
}

// Implement the json.Marshaler interface
func (c *Column) MarshalJSON() ([]byte, error) {
	buffer := columnJsonBufferPool.Get().(*bytes.Buffer)
	defer columnJsonBufferPool.Put(buffer)
	buffer.Reset()

	encoder := json.NewEncoder(buffer)

	if err := encoder.Encode(c.ColumnValue); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
