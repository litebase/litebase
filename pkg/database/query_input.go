package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
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
	ID            string                       `json:"id" validate:"required,min=1"`
	Parameters    []sqlite3.StatementParameter `json:"parameters" validate:"omitempty,required,dive"`
	Statement     string                       `json:"statement" validate:"required"`
	TransactionID string                       `json:"transaction_id" validate:"omitempty,required"`
}

func NewQueryInput(
	id string,
	statement string,
	parameters []sqlite3.StatementParameter,
	transactionID string,
) *QueryInput {
	return &QueryInput{
		ID:            id,
		Statement:     statement,
		Parameters:    parameters,
		TransactionID: transactionID,
	}
}

func (q *QueryInput) Decode(buffer, parametersBuffer *bytes.Buffer) error {
	// Read the length of the id
	idLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))
	q.ID = string(buffer.Next(idLength))

	// Read the length of the transaction id
	transactionIDLength := int(binary.LittleEndian.Uint32(buffer.Next(4)))

	if transactionIDLength > 0 {
		q.TransactionID = string(buffer.Next(transactionIDLength))
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

func (q *QueryInput) Encode(buffer *bytes.Buffer) []byte {
	buffer.Reset()

	// Write the length of the id
	var idBytes [4]byte

	idLengthUint32, err := utils.SafeIntToUint32(len(q.ID))

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint32(idBytes[:], idLengthUint32)
	buffer.Write(idBytes[:])

	// Write the id
	buffer.Write([]byte(q.ID))

	if q.TransactionID != "" {
		// Write the length of the transaction id
		var transactionIDLengthBytes [4]byte

		transactionIDLenUint32, err := utils.SafeIntToUint32(len(q.TransactionID))

		if err != nil {
			return nil
		}

		binary.LittleEndian.PutUint32(transactionIDLengthBytes[:], transactionIDLenUint32)
		buffer.Write(transactionIDLengthBytes[:])

		// Write the transaction id
		buffer.Write([]byte(q.TransactionID))
	} else {
		// Write the length of the transaction id
		var transactionIDLengthBytes [4]byte
		binary.LittleEndian.PutUint32(transactionIDLengthBytes[:], uint32(0))
		buffer.Write(transactionIDLengthBytes[:])
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
	q.ID = ""
	q.Statement = ""
	q.Parameters = q.Parameters[:0]
	q.TransactionID = ""
}

// UnmarshalJSON implements the json.Unmarshaler interface for QueryInput.
func (q *QueryInput) UnmarshalJSON(jsonData []byte) error {
	var data map[string]any

	if err := json.Unmarshal(jsonData, &data); err != nil {
		return fmt.Errorf("failed to unmarshal QueryInput: %w", err)
	}

	if data["id"] != nil {
		q.ID = data["id"].(string)
	}

	if data["transaction_id"] != nil {
		q.TransactionID = data["transaction_id"].(string)
	}

	if data["statement"] != nil {
		if statement, ok := data["statement"].(string); ok {
			q.Statement = statement
		}
	}

	if data["parameters"] != nil {
		parameters, ok := data["parameters"].([]any)

		if !ok {
			return fmt.Errorf("invalid parameters format")
		}

		for _, parameter := range parameters {
			if _, ok := parameter.(map[string]any)["type"]; !ok {
				return fmt.Errorf("invalid parameter type")
			}

			if _, ok := parameter.(map[string]any)["value"]; !ok {
				continue
			}

			if parameter.(map[string]any)["type"] == "TEXT" {
				if textValue, ok := parameter.(map[string]any)["value"].(string); ok {
					parameter.(map[string]any)["value"] = []byte(textValue)
				}
			}

			// Handle INTEGER values that may be in scientific notation
			if parameter.(map[string]any)["type"] == "INTEGER" {
				if integerValue, ok := parameter.(map[string]any)["value"].(float64); ok {
					parameter.(map[string]any)["value"] = int64(integerValue)
				}
			}

			if parameter.(map[string]any)["type"] == "FLOAT" {
				if floatValue, ok := parameter.(map[string]any)["value"].(float64); ok {
					parameter.(map[string]any)["value"] = floatValue
				}
			}

			if parameter.(map[string]any)["type"] == "BLOB" {
				if blobValue, ok := parameter.(map[string]any)["value"].([]byte); ok {
					parameter.(map[string]any)["value"] = blobValue
				}
			}

			// Append the parameter to the query
			q.Parameters = append(q.Parameters, sqlite3.StatementParameter{
				Type:  parameter.(map[string]any)["type"].(string),
				Value: parameter.(map[string]any)["value"],
			})
		}
	}

	return nil
}
