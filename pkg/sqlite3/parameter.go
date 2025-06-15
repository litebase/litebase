package sqlite3

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
)

const (
	ParameterTypeText    = "TEXT"
	ParameterTypeInteger = "INTEGER"
	ParameterTypeFloat   = "FLOAT"
	ParameterTypeBlob    = "BLOB"
	ParameterTypeNull    = "NULL"
)

type RawParameter struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

var rawParameterPool = sync.Pool{
	New: func() interface{} {
		return &RawParameter{}
	},
}

var jsonParameterDecoderPool = JsonParameterDecoderPool()

type StatementParameter struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (sp StatementParameter) Encode(buffer *bytes.Buffer) []byte {
	buffer.Reset()

	// Write the type and value length
	switch sp.Type {
	case "INTEGER":
		buffer.WriteByte(uint8(ColumnTypeInteger))

		var valueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(valueLengthBytes[:], uint32(8))
		buffer.Write(valueLengthBytes[:])

		var valueBytes [8]byte
		binary.LittleEndian.PutUint64(valueBytes[:], uint64(sp.Value.(int64)))
		buffer.Write(valueBytes[:])
	case "FLOAT":
		buffer.WriteByte(uint8(ColumnTypeFloat))

		var valueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(valueLengthBytes[:], uint32(8))
		buffer.Write(valueLengthBytes[:])

		var valueBytes [8]byte
		binary.LittleEndian.PutUint64(valueBytes[:], math.Float64bits(sp.Value.(float64)))
		buffer.Write(valueBytes[:])
	case "TEXT":
		buffer.WriteByte(uint8(ColumnTypeText))

		var valueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(valueLengthBytes[:], uint32(len(sp.Value.(string))))
		buffer.Write(valueLengthBytes[:])

		buffer.Write([]byte(sp.Value.(string)))
	case "BLOB":
		buffer.WriteByte(uint8(ColumnTypeBlob))

		var valueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(valueLengthBytes[:], uint32(len(sp.Value.([]byte))))
		buffer.Write(valueLengthBytes[:])

		buffer.Write(sp.Value.([]byte))
	case "NULL":
		buffer.WriteByte(uint8(ColumnTypeNull))

		var valueLengthBytes [4]byte
		binary.LittleEndian.PutUint32(valueLengthBytes[:], uint32(0))
		buffer.Write(valueLengthBytes[:])
	}

	return buffer.Bytes()
}

func DecodeStatementParameter(buffer *bytes.Buffer) (StatementParameter, error) {
	var sp StatementParameter

	// Read the type length
	parameterType := ColumnType(buffer.Next(1)[0])
	valueLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))

	// Read the value
	switch parameterType {
	case ColumnTypeInteger:
		sp.Type = "INTEGER"
		sp.Value = int64(binary.LittleEndian.Uint64(buffer.Next(8)))
	case ColumnTypeFloat:
		sp.Type = "FLOAT"
		sp.Value = math.Float64frombits(binary.LittleEndian.Uint64(buffer.Next(8)))
	case ColumnTypeText:
		sp.Type = "TEXT"
		sp.Value = buffer.Next(valueLength)
	case ColumnTypeBlob:
		sp.Type = "BLOB"
		sp.Value = buffer.Next(valueLength)
	case ColumnTypeNull:
		sp.Type = "NULL"
		sp.Value = nil
	}

	return sp, nil
}

func (qp *StatementParameter) UnmarshalJSON(data []byte) error {
	raw := rawParameterPool.Get().(*RawParameter)
	defer rawParameterPool.Put(raw)

	decoder := jsonParameterDecoderPool.Get()
	defer jsonParameterDecoderPool.Put(decoder)

	decoder.Buffer.Write(data)

	// Reset the fields of the struct
	raw.Type = ""
	raw.Value = nil

	if err := decoder.JsonDecoder.Decode(raw); err != nil {
		return err
	}

	qp.Type = raw.Type
	qp.Value = raw.Value

	return nil
}
