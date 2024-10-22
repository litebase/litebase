package sqlite3_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"litebase/server/sqlite3"
	"math"
	"testing"
)

func TestNewColumn(t *testing.T) {
	testCases := []struct {
		columnType sqlite3.ColumnType
		value      interface{}
	}{
		{
			sqlite3.ColumnTypeInteger,
			1,
		},
		{
			sqlite3.ColumnTypeFloat,
			1.0001,
		},
		{
			sqlite3.ColumnTypeText,
			"This is some text",
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
	testCases := []struct {
		columnType sqlite3.ColumnType
		value      interface{}
	}{
		{
			sqlite3.ColumnTypeInteger,
			1,
		},
		{
			sqlite3.ColumnTypeFloat,
			1.0001,
		},
		{
			sqlite3.ColumnTypeText,
			"This is some text",
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

			data, err := column.Encode(buffer)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

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

			length := int(data[1]) | int(data[2])<<8 | int(data[3])<<16 | int(data[4])<<24

			if length != len(data)-5 {
				t.Fatalf(
					"expected length to be %d, got %d",
					len(data)-5,
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
				if length != 4 {
					t.Fatalf("expected length to be 4, got %d", length)
				}

				value := int(data[5]) | int(data[6])<<8 | int(data[7])<<16 | int(data[8])<<24

				if value != testCase.value {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						value,
					)
				}

			case sqlite3.ColumnTypeFloat:
				if length != 8 {
					t.Fatalf("expected length to be 8, got %d", length)
				}

				value := math.Float64frombits(binary.LittleEndian.Uint64(data[5:]))

				if value != testCase.value {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						value,
					)
				}

			case sqlite3.ColumnTypeText:
				if length != len(testCase.value.(string)) {
					t.Fatalf(
						"expected length to be %d, got %d",
						len(testCase.value.(string)),
						length,
					)
				}

				value := string(data[5:])

				if value != testCase.value {
					t.Fatalf(
						"expected value to be %v, got %v",
						testCase.value,
						value,
					)
				}

			case sqlite3.ColumnTypeBlob:
				if length != len(testCase.value.([]byte)) {
					t.Fatalf(
						"expected length to be %d, got %d",
						len(testCase.value.([]byte)),
						length,
					)
				}

				value := string(data[5:])
				expectedValue := string(testCase.value.([]byte))

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
