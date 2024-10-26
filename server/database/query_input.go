package database

import (
	"bytes"
	"encoding/binary"
	"litebase/server/sqlite3"
)

/*
QueryInput is a struct that represents the input of a query.

| Offset      | Length | Description                           |
|-------------|--------|---------------------------------------|
| 0           | 4      | The length of the id                  |
| 4           | n      | The unique identifier for the query   |
| 4 + n       | 4      | The length of the statement           |
| 8 + n       | m      | The statement to execute              |
| 8 + n + m   | 4      | The length of the parameters array    |
| 12 + n + m  | p      | The parameters to bind to the statement |
*/
type QueryInput struct {
	Id         []byte                       `json:"id"`
	Statement  []byte                       `json:"statement"`
	Parameters []sqlite3.StatementParameter `json:"parameters"`
}

func NewQueryInput(id []byte, statement []byte, parameters []sqlite3.StatementParameter) *QueryInput {
	return &QueryInput{
		Id:         id,
		Statement:  statement,
		Parameters: parameters,
	}
}

func (q *QueryInput) Decode(buffer *bytes.Buffer) error {
	// Read the length of the id
	idLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))
	q.Id = buffer.Next(idLength)

	// Read the length of the statement
	statementLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))
	q.Statement = buffer.Next(statementLength)

	// Read the length of the parameters array
	parametersLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))

	// Parameters Buffer
	parametersBuffer := bytes.NewBuffer(buffer.Next(parametersLength))

	// Read the parameters
	for parametersBuffer.Len() > 0 {
		parameter, err := sqlite3.DecodeStatementParameter(parametersBuffer)

		if err != nil {
			return err
		}

		q.Parameters = append(q.Parameters, parameter)
	}

	return nil
}

func (q *QueryInput) Encode(buffer *bytes.Buffer) []byte {
	buffer.Reset()

	// Write the length of the id
	var idBytes [4]byte
	binary.LittleEndian.PutUint32(idBytes[:], uint32(len(q.Id)))
	buffer.Write(idBytes[:])

	// Write the id
	buffer.Write([]byte(q.Id))

	// Write the length of the statement
	var statementLengthBytes [4]byte
	binary.LittleEndian.PutUint32(statementLengthBytes[:], uint32(len(q.Statement)))
	buffer.Write(statementLengthBytes[:])

	// Write the statement
	buffer.Write([]byte(q.Statement))

	parametersBuffer := bytes.NewBuffer(nil)
	parameterBuffer := bytes.NewBuffer(nil)

	for _, parameter := range q.Parameters {
		parametersBuffer.Write(parameter.Encode(parameterBuffer))
	}

	// Write the length of the parameters array
	var parametersLengthBytes [4]byte
	binary.LittleEndian.PutUint32(parametersLengthBytes[:], uint32(parametersBuffer.Len()))
	buffer.Write(parametersLengthBytes[:])

	// Write the parameters array
	buffer.Write(parametersBuffer.Bytes())

	return buffer.Bytes()
}

func (q *QueryInput) Reset() {
	q.Id = nil
	q.Statement = nil
	q.Parameters = nil
}
