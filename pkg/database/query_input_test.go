package database

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryInputUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name           string
		jsonInput      string
		expectedID     string
		expectedStmt   string
		expectedParams int
		expectError    bool
		validateParams func(t *testing.T, params []sqlite3.StatementParameter)
	}{
		{
			name: "Basic query with TEXT parameter",
			jsonInput: `{
				"id": "query-1",
				"statement": "SELECT * FROM users WHERE name = ?",
				"parameters": [
					{"type": "TEXT", "value": "John Doe"}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-1",
			expectedStmt:   "SELECT * FROM users WHERE name = ?",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "TEXT" {
					t.Errorf("Expected type TEXT, got %s", params[0].Type)
				}
				if string(params[0].Value.([]byte)) != "John Doe" {
					t.Errorf("Expected value 'John Doe', got %v", params[0].Value)
				}
			},
		},
		{
			name: "Query with INTEGER parameter",
			jsonInput: `{
				"id": "query-2",
				"statement": "SELECT * FROM users WHERE id = ?",
				"parameters": [
					{"type": "INTEGER", "value": 42}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-2",
			expectedStmt:   "SELECT * FROM users WHERE id = ?",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "INTEGER" {
					t.Errorf("Expected type INTEGER, got %s", params[0].Type)
				}
				if params[0].Value.(int64) != 42 {
					t.Errorf("Expected value 42, got %v", params[0].Value)
				}
			},
		},
		{
			name: "Query with FLOAT parameter",
			jsonInput: `{
				"id": "query-3",
				"statement": "SELECT * FROM products WHERE price < ?",
				"parameters": [
					{"type": "FLOAT", "value": 99.99}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-3",
			expectedStmt:   "SELECT * FROM products WHERE price < ?",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "FLOAT" {
					t.Errorf("Expected type FLOAT, got %s", params[0].Type)
				}
				if params[0].Value.(float64) != 99.99 {
					t.Errorf("Expected value 99.99, got %v", params[0].Value)
				}
			},
		},
		{
			name: "Query with BLOB parameter (base64 encoded)",
			jsonInput: `{
				"id": "query-4",
				"statement": "INSERT INTO files (content) VALUES (?)",
				"parameters": [
					{"type": "BLOB", "value": "SGVsbG8gV29ybGQ="}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-4",
			expectedStmt:   "INSERT INTO files (content) VALUES (?)",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "BLOB" {
					t.Errorf("Expected type BLOB, got %s", params[0].Type)
				}
				expected := []byte("Hello World")
				actual := params[0].Value.([]byte)
				if !bytes.Equal(actual, expected) {
					t.Errorf("Expected decoded blob %v, got %v", expected, actual)
				}
			},
		},
		{
			name: "Query with empty BLOB parameter",
			jsonInput: `{
				"id": "query-5",
				"statement": "INSERT INTO files (content) VALUES (?)",
				"parameters": [
					{"type": "BLOB", "value": ""}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-5",
			expectedStmt:   "INSERT INTO files (content) VALUES (?)",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "BLOB" {
					t.Errorf("Expected type BLOB, got %s", params[0].Type)
				}
				actual := params[0].Value.([]byte)
				if len(actual) != 0 {
					t.Errorf("Expected empty blob, got %v", actual)
				}
			},
		},
		{
			name: "Query with NULL parameter",
			jsonInput: `{
				"id": "query-6",
				"statement": "UPDATE users SET email = ? WHERE id = ?",
				"parameters": [
					{"type": "NULL"},
					{"type": "INTEGER", "value": 1}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-6",
			expectedStmt:   "UPDATE users SET email = ? WHERE id = ?",
			expectedParams: 2,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "NULL" {
					t.Errorf("Expected type NULL, got %s", params[0].Type)
				}
				if params[0].Value != nil {
					t.Errorf("Expected nil value for NULL, got %v", params[0].Value)
				}
				if params[1].Type != "INTEGER" {
					t.Errorf("Expected type INTEGER, got %s", params[1].Type)
				}
			},
		},
		{
			name: "Query with multiple parameters of different types",
			jsonInput: `{
				"id": "query-7",
				"statement": "INSERT INTO users (name, age, balance, avatar) VALUES (?, ?, ?, ?)",
				"parameters": [
					{"type": "TEXT", "value": "Alice"},
					{"type": "INTEGER", "value": 30},
					{"type": "FLOAT", "value": 1000.50},
					{"type": "BLOB", "value": "iVBORw0KGgo="}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-7",
			expectedStmt:   "INSERT INTO users (name, age, balance, avatar) VALUES (?, ?, ?, ?)",
			expectedParams: 4,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "TEXT" || string(params[0].Value.([]byte)) != "Alice" {
					t.Errorf("Parameter 0 validation failed")
				}
				if params[1].Type != "INTEGER" || params[1].Value.(int64) != 30 {
					t.Errorf("Parameter 1 validation failed")
				}
				if params[2].Type != "FLOAT" || params[2].Value.(float64) != 1000.50 {
					t.Errorf("Parameter 2 validation failed")
				}
				if params[3].Type != "BLOB" {
					t.Errorf("Parameter 3 validation failed")
				}
			},
		},
		{
			name: "Query with transaction ID",
			jsonInput: `{
				"id": "query-8",
				"statement": "INSERT INTO users (name) VALUES (?)",
				"parameters": [
					{"type": "TEXT", "value": "Bob"}
				],
				"transactionId": "tx-12345"
			}`,
			expectedID:     "query-8",
			expectedStmt:   "INSERT INTO users (name) VALUES (?)",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "TEXT" {
					t.Errorf("Expected type TEXT, got %s", params[0].Type)
				}
			},
		},
		{
			name: "Query with no parameters",
			jsonInput: `{
				"id": "query-9",
				"statement": "SELECT * FROM users",
				"parameters": [],
				"transactionId": ""
			}`,
			expectedID:     "query-9",
			expectedStmt:   "SELECT * FROM users",
			expectedParams: 0,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				// No parameters to validate
			},
		},
		{
			name: "Query with large BLOB (1KB)",
			jsonInput: func() string {
				largeData := make([]byte, 1024)
				for i := range largeData {
					largeData[i] = byte(i % 256)
				}
				base64Data := base64.StdEncoding.EncodeToString(largeData)
				return `{
					"id": "query-10",
					"statement": "INSERT INTO files (content) VALUES (?)",
					"parameters": [
						{"type": "BLOB", "value": "` + base64Data + `"}
					],
					"transactionId": ""
				}`
			}(),
			expectedID:     "query-10",
			expectedStmt:   "INSERT INTO files (content) VALUES (?)",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "BLOB" {
					t.Errorf("Expected type BLOB, got %s", params[0].Type)
				}
				actual := params[0].Value.([]byte)
				if len(actual) != 1024 {
					t.Errorf("Expected blob length 1024, got %d", len(actual))
				}
				// Verify pattern
				for i := 0; i < len(actual); i++ {
					if actual[i] != byte(i%256) {
						t.Errorf("Blob data pattern mismatch at index %d", i)
						break
					}
				}
			},
		},
		{
			name: "Query with INTEGER as string (large value beyond JS safe range)",
			jsonInput: `{
				"id": "query-11",
				"statement": "SELECT * FROM data WHERE id = ?",
				"parameters": [
					{"type": "INTEGER", "value": "9007199254740993"}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-11",
			expectedStmt:   "SELECT * FROM data WHERE id = ?",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "INTEGER" {
					t.Errorf("Expected type INTEGER, got %s", params[0].Type)
				}
				expected := int64(9007199254740993)
				actual := params[0].Value.(int64)
				if actual != expected {
					t.Errorf("Expected value %d, got %d", expected, actual)
				}
			},
		},
		{
			name: "Query with negative INTEGER as string",
			jsonInput: `{
				"id": "query-12",
				"statement": "SELECT * FROM data WHERE offset = ?",
				"parameters": [
					{"type": "INTEGER", "value": "-9223372036854775808"}
				],
				"transactionId": ""
			}`,
			expectedID:     "query-12",
			expectedStmt:   "SELECT * FROM data WHERE offset = ?",
			expectedParams: 1,
			validateParams: func(t *testing.T, params []sqlite3.StatementParameter) {
				if params[0].Type != "INTEGER" {
					t.Errorf("Expected type INTEGER, got %s", params[0].Type)
				}
				expected := int64(-9223372036854775808)
				actual := params[0].Value.(int64)
				if actual != expected {
					t.Errorf("Expected value %d, got %d", expected, actual)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var queryInput QueryInput

			err := json.Unmarshal([]byte(tc.jsonInput), &queryInput)

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Validate ID
			if queryInput.ID != tc.expectedID {
				t.Errorf("Expected ID %s, got %s", tc.expectedID, queryInput.ID)
			}

			// Validate statement
			if queryInput.Statement != tc.expectedStmt {
				t.Errorf("Expected statement %s, got %s", tc.expectedStmt, queryInput.Statement)
			}

			// Validate parameter count
			if len(queryInput.Parameters) != tc.expectedParams {
				t.Errorf("Expected %d parameters, got %d", tc.expectedParams, len(queryInput.Parameters))
			}

			// Validate parameters
			if tc.validateParams != nil {
				tc.validateParams(t, queryInput.Parameters)
			}
		})
	}
}

func TestQueryInputUnmarshalJSON_InvalidBlob(t *testing.T) {
	testCases := []struct {
		name      string
		jsonInput string
	}{
		{
			name: "Invalid base64 encoding",
			jsonInput: `{
				"id": "query-1",
				"statement": "INSERT INTO files (content) VALUES (?)",
				"parameters": [
					{"type": "BLOB", "value": "Not!Valid!Base64!!!"}
				],
				"transactionId": ""
			}`,
		},
		{
			name: "Invalid integer string",
			jsonInput: `{
				"id": "query-2",
				"statement": "SELECT * FROM data WHERE id = ?",
				"parameters": [
					{"type": "INTEGER", "value": "not-a-number"}
				],
				"transactionId": ""
			}`,
		},
		{
			name: "Integer string overflow",
			jsonInput: `{
				"id": "query-3",
				"statement": "SELECT * FROM data WHERE id = ?",
				"parameters": [
					{"type": "INTEGER", "value": "99999999999999999999999999999"}
				],
				"transactionId": ""
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var queryInput QueryInput

			err := json.Unmarshal([]byte(tc.jsonInput), &queryInput)

			if err == nil {
				t.Fatalf("Expected error for invalid input, but got none")
			}
		})
	}
}

func TestQueryInputEncodeAndDecode(t *testing.T) {
	testCases := []struct {
		name       string
		id         string
		statement  string
		parameters []sqlite3.StatementParameter
		txID       string
	}{
		{
			name:      "Simple query with TEXT parameter",
			id:        "test-1",
			statement: "SELECT * FROM users WHERE name = ?",
			parameters: []sqlite3.StatementParameter{
				{Type: "TEXT", Value: []byte("John Doe")},
			},
			txID: "",
		},
		{
			name:      "Query with multiple parameter types",
			id:        "test-2",
			statement: "INSERT INTO users (name, age, balance) VALUES (?, ?, ?)",
			parameters: []sqlite3.StatementParameter{
				{Type: "TEXT", Value: []byte("Alice")},
				{Type: "INTEGER", Value: int64(30)},
				{Type: "FLOAT", Value: float64(1000.50)},
			},
			txID: "",
		},
		{
			name:      "Query with BLOB parameter",
			id:        "test-3",
			statement: "INSERT INTO files (content) VALUES (?)",
			parameters: []sqlite3.StatementParameter{
				{Type: "BLOB", Value: []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}},
			},
			txID: "",
		},
		{
			name:      "Query with transaction ID",
			id:        "test-4",
			statement: "UPDATE users SET name = ? WHERE id = ?",
			parameters: []sqlite3.StatementParameter{
				{Type: "TEXT", Value: []byte("Bob")},
				{Type: "INTEGER", Value: int64(1)},
			},
			txID: "tx-12345",
		},
		{
			name:       "Query with no parameters",
			id:         "test-5",
			statement:  "SELECT * FROM users",
			parameters: []sqlite3.StatementParameter{},
			txID:       "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create original query input
			original := NewQueryInput(tc.id, tc.statement, tc.parameters, tc.txID)

			// Encode to bytes
			buffer := new(bytes.Buffer)
			encoded := original.Encode(buffer)

			if encoded == nil {
				t.Fatalf("Encode returned nil")
			}

			// Decode back
			decoded := &QueryInput{}
			paramsBuffer := new(bytes.Buffer)
			err := decoded.Decode(bytes.NewBuffer(encoded), paramsBuffer)

			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Verify ID
			if decoded.ID != original.ID {
				t.Errorf("ID mismatch: expected %s, got %s", original.ID, decoded.ID)
			}

			// Verify statement
			if decoded.Statement != original.Statement {
				t.Errorf("Statement mismatch: expected %s, got %s", original.Statement, decoded.Statement)
			}

			// Verify transaction ID
			if decoded.TransactionID != original.TransactionID {
				t.Errorf("Transaction ID mismatch: expected %s, got %s", original.TransactionID, decoded.TransactionID)
			}

			// Verify parameter count
			if len(decoded.Parameters) != len(original.Parameters) {
				t.Errorf("Parameter count mismatch: expected %d, got %d", len(original.Parameters), len(decoded.Parameters))
			}

			// Verify each parameter
			for i, origParam := range original.Parameters {
				if i >= len(decoded.Parameters) {
					t.Errorf("Missing parameter at index %d", i)
					continue
				}

				decodedParam := decoded.Parameters[i]

				if decodedParam.Type != origParam.Type {
					t.Errorf("Parameter %d type mismatch: expected %s, got %s", i, origParam.Type, decodedParam.Type)
				}

				// Compare values based on type
				switch origParam.Type {
				case "TEXT", "BLOB":
					origBytes := origParam.Value.([]byte)
					decodedBytes := decodedParam.Value.([]byte)
					if !bytes.Equal(origBytes, decodedBytes) {
						t.Errorf("Parameter %d value mismatch: expected %v, got %v", i, origBytes, decodedBytes)
					}
				case "INTEGER":
					if origParam.Value.(int64) != decodedParam.Value.(int64) {
						t.Errorf("Parameter %d value mismatch: expected %v, got %v", i, origParam.Value, decodedParam.Value)
					}
				case "FLOAT":
					if origParam.Value.(float64) != decodedParam.Value.(float64) {
						t.Errorf("Parameter %d value mismatch: expected %v, got %v", i, origParam.Value, decodedParam.Value)
					}
				case "NULL":
					if decodedParam.Value != nil {
						t.Errorf("Parameter %d expected nil for NULL type, got %v", i, decodedParam.Value)
					}
				}
			}
		})
	}
}

func TestQueryInputReset(t *testing.T) {
	// Create a query input with data
	queryInput := NewQueryInput(
		"test-id",
		"SELECT * FROM users WHERE id = ?",
		[]sqlite3.StatementParameter{
			{Type: "INTEGER", Value: int64(1)},
		},
		"tx-123",
	)

	// Reset it
	queryInput.Reset()

	// Verify all fields are reset
	if queryInput.ID != "" {
		t.Errorf("Expected empty ID after reset, got %s", queryInput.ID)
	}

	if queryInput.Statement != "" {
		t.Errorf("Expected empty statement after reset, got %s", queryInput.Statement)
	}

	if queryInput.TransactionID != "" {
		t.Errorf("Expected empty transaction ID after reset, got %s", queryInput.TransactionID)
	}

	if len(queryInput.Parameters) != 0 {
		t.Errorf("Expected empty parameters after reset, got %d parameters", len(queryInput.Parameters))
	}
}
