package sqlite3_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"testing"

	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestNewColumn(t *testing.T) {
	var columnValueBytes [8]byte
	binary.LittleEndian.PutUint64(columnValueBytes[:], math.Float64bits(1.0001))

	testCases := []struct {
		columnType sqlite3.ColumnType
		value      []byte
	}{
		{
			sqlite3.ColumnTypeInteger,
			[]byte{0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			sqlite3.ColumnTypeFloat,
			columnValueBytes[:],
		},
		{
			sqlite3.ColumnTypeText,
			[]byte("This is some text"),
		},
		{
			sqlite3.ColumnTypeBlob,
			[]byte("This is a blob"),
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Testing %v column type", testCase.columnType), func(t *testing.T) {
			column := sqlite3.NewColumn(testCase.columnType, testCase.value)

			if column.ColumnType != testCase.columnType {
				t.Fatalf(
					"expected column type %v,  got %v",
					testCase.columnType,
					column.ColumnType,
				)
			}
		})
	}
}

func TestColumnEncode(t *testing.T) {
	var int64ValueBytes [8]byte
	binary.LittleEndian.PutUint64(int64ValueBytes[:], uint64(1))

	var floatValueBytes [8]byte
	binary.LittleEndian.PutUint64(floatValueBytes[:], math.Float64bits(1.0001))

	testCases := []struct {
		columnType sqlite3.ColumnType
		value      []byte
	}{
		{
			sqlite3.ColumnTypeInteger,
			int64ValueBytes[:],
		},
		{
			sqlite3.ColumnTypeFloat,
			floatValueBytes[:],
		},
		{
			sqlite3.ColumnTypeText,
			[]byte("This is some text"),
		},
		{
			sqlite3.ColumnTypeBlob,
			[]byte("This is a blob"),
		},
		{
			sqlite3.ColumnTypeNull,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("Testing %v column type", testCase.columnType), func(t *testing.T) {
			column := sqlite3.NewColumn(testCase.columnType, testCase.value)

			buffer := new(bytes.Buffer)

			err := column.Encode(buffer)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			data := buffer.Bytes()

			if data == nil {
				t.Fatalf("expected data to be not nil")
			}

			// Ensure the first byte is the column type
			if data[0] != byte(testCase.columnType) {
				t.Fatalf(
					"expected first byte for column type %d to be %v, got %v",
					testCase.columnType,
					byte(testCase.columnType),
					data[0],
				)
			}

			// Ensure the next 4 bytes are the length of the column value
			if len(data) < 5 {
				t.Fatalf("expected data to be at least 5 bytes long")
			}

			length := int(binary.LittleEndian.Uint32(data[1:5]))

			if length != len(data[5:]) {
				log.Println(data)
				t.Fatalf(
					"expected length to be %d, got %d",
					len(data[5:]),
					length,
				)
			}

			// Ensure the rest of the data is the column value
			if len(data) != 5+length {
				t.Fatalf(
					"expected data to be %d bytes long, got %d",
					5+length,
					len(data),
				)
			}

			switch testCase.columnType {
			case sqlite3.ColumnTypeInteger:
				if length != 8 {
					t.Fatalf("expected length to be 8, got %d", length)
				}

				// value := int(data[5]) | int(data[6])<<8 | int(data[7])<<16 | int(data[8])<<24
				valueBytes := make([]byte, 8)
				binary.LittleEndian.PutUint32(valueBytes, uint32(data[5]))

				if !bytes.Equal(valueBytes, testCase.value) {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						valueBytes,
					)
				}

			case sqlite3.ColumnTypeFloat:
				if length != 8 {
					t.Fatalf("expected length to be 8, got %d", length)
				}

				valueBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(valueBytes, binary.LittleEndian.Uint64(data[5:]))

				if !bytes.Equal(valueBytes, testCase.value) {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						valueBytes,
					)
				}

			case sqlite3.ColumnTypeText:
				if length != len(testCase.value) {
					t.Fatalf(
						"expected length to be %d, got %d",
						len(testCase.value),
						length,
					)
				}

				value := (data[5:])

				if !bytes.Equal(value, testCase.value) {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						value,
					)
				}

			case sqlite3.ColumnTypeBlob:
				if length != len(testCase.value) {
					t.Fatalf(
						"expected length to be %d, got %d",
						len(testCase.value),
						length,
					)
				}

				value := string(data[5:])
				expectedValue := string(testCase.value)

				if value != expectedValue {
					t.Fatalf(
						"expected value to be %v, got %v",
						expectedValue,
						value,
					)
				}

			case sqlite3.ColumnTypeNull:
				if length != 0 {
					t.Fatalf("expected length to be 0, got %d", length)
				}

				if len(data) != 5 {
					t.Fatalf("expected data to be 5 bytes long")
				}

			default:
				t.Fatalf("unexpected column type: %v", testCase.columnType)
			}
		})
	}
}
