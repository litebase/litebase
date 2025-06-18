package database

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/sqlite3"
)

/*
QueryInput is a struct that represents the input of a query.

| Offset          | Length | Description                           |
|-----------------|--------|---------------------------------------|
| 0               | 4      | The length of the id                  |
| 4               | n      | The unique identifier for the query   |
| 4 + n           | 4      | The length of the statement           |
| 8 + n           | m      | The statement to execute              |
| 8 + n + m       | 4      | The length of the parameters array    |
| 12 + n + m      | p      | The parameters to bind to the statement |
| 12 + n + m + p  | 4      | The length of the transaction id       |
| 16 + n + m + p  | q      | The transaction id                    |
*/
type QueryInput struct {
	Id            string                       `json:"id" validate:"required"`
	Parameters    []sqlite3.StatementParameter `json:"parameters" validate:"dive"`
	Statement     string                       `json:"statement" validate:"required,min=1"`
	TransactionId string                       `json:"transaction_id"`
}

func NewQueryInput(
	id string,
	statement string,
	parameters []sqlite3.StatementParameter,
	transactionId string,
) *QueryInput {
	return &QueryInput{
		Id:            id,
		Statement:     statement,
		Parameters:    parameters,
		TransactionId: transactionId,
	}
}

func (q *QueryInput) Decode(buffer, parametersBuffer *bytes.Buffer) error {
	// Read the length of the id
	idLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))
	q.Id = string(buffer.Next(idLength))

	// Read the length of the transaction id
	transactionIdLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))

	if transactionIdLength > 0 {
		q.TransactionId = string(buffer.Next(transactionIdLength))
	}

	// Read the length of the statement
	statementLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))
	q.Statement = string(buffer.Next(statementLength))

	// Read the length of the parameters array
	parametersLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))

	// Parameters Buffer
	parametersBuffer.Write(buffer.Next(parametersLength))

	parameterIndex := 0

	// Read the parameters
	for parametersBuffer.Len() > 0 {
		parameter, err := sqlite3.DecodeStatementParameter(parametersBuffer)

		if err != nil {
			return err
		}

		if parameterIndex >= len(q.Parameters) {
			q.Parameters = append(q.Parameters, parameter)
		} else {
			q.Parameters[parameterIndex] = parameter
		}

		parameterIndex++
	}

	return nil
}

func (q *QueryInput) DecodeFromMap(data map[string]any) error {
	if data["id"] != nil {
		q.Id = data["id"].(string)
	}

	if data["transaction_id"] != nil {
		q.TransactionId = data["transaction_id"].(string)
	}

	if data["statement"] != nil {
		q.Statement = data["statement"].(string)
	}

	if data["parameters"] != nil {
		parameters, ok := data["parameters"].([]any)

		if !ok {
			return fmt.Errorf("invalid parameters format")
		}

		for _, parameter := range parameters {
			if _, ok := parameter.(map[string]any)["type"]; !ok {
				return fmt.Errorf("invalid parameter format")
			}

			if parameter.(map[string]any)["type"] == "TEXT" {
				parameter.(map[string]any)["value"] = []byte(parameter.(map[string]any)["value"].(string))
			}

			// Handle INTEGER values that may be in scientific notation
			if parameter.(map[string]any)["type"] == "INTEGER" {
				parameter.(map[string]any)["value"] = int64(parameter.(map[string]any)["value"].(float64))
			}

			q.Parameters = append(q.Parameters, sqlite3.StatementParameter{
				Type:  parameter.(map[string]any)["type"].(string),
				Value: parameter.(map[string]any)["value"],
			})
		}
	}

	return nil
}

func (q *QueryInput) Encode(buffer *bytes.Buffer) []byte {
	buffer.Reset()

	// Write the length of the id
	var idBytes [4]byte

	idLengthUint32, err := utils.SafeIntToUint32(len(q.Id))

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint32(idBytes[:], idLengthUint32)
	buffer.Write(idBytes[:])

	// Write the id
	buffer.Write([]byte(q.Id))

	if q.TransactionId != "" {
		// Write the length of the transaction id
		var transactionIdLengthBytes [4]byte

		transactionIDLenUint32, err := utils.SafeIntToUint32(len(q.TransactionId))

		if err != nil {
			return nil
		}

		binary.LittleEndian.PutUint32(transactionIdLengthBytes[:], transactionIDLenUint32)
		buffer.Write(transactionIdLengthBytes[:])

		// Write the transaction id
		buffer.Write([]byte(q.TransactionId))
	} else {
		// Write the length of the transaction id
		var transactionIdLengthBytes [4]byte
		binary.LittleEndian.PutUint32(transactionIdLengthBytes[:], uint32(0))
		buffer.Write(transactionIdLengthBytes[:])
	}

	// Write the length of the statement
	var statementLengthBytes [4]byte

	statementLenUint32, err := utils.SafeIntToUint32(len(q.Statement))

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint32(statementLengthBytes[:], statementLenUint32)

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

	parametersLenUint32, err := utils.SafeIntToUint32(parametersBuffer.Len())

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint32(parametersLengthBytes[:], parametersLenUint32)
	buffer.Write(parametersLengthBytes[:])

	// Write the parameters array
	buffer.Write(parametersBuffer.Bytes())

	return buffer.Bytes()
}

func (q *QueryInput) Reset() {
	q.Id = ""
	q.Statement = ""
	q.Parameters = q.Parameters[:0]
	q.TransactionId = ""
}
