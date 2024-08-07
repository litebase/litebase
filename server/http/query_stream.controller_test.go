package http_test

import (
	"litebase/server/http"
	"litebase/server/query"
	"testing"
)

func TestParseQueryStatementAndParameters(t *testing.T) {
	var statement = make([]byte, 16*1024*1024) // 16 Mib
	var parameters = make([]interface{}, 999)
	var statementLength, count int
	var err error

	tests := []struct {
		input  query.QueryInput
		output []interface{}
		count  int
	}{
		{
			query.QueryInput{
				Statement:  "SELECT * FROM users WHERE id = ? LIMIT ?",
				Parameters: []string{"i:100", "i:1"},
			},
			[]interface{}{100, 1},
			2,
		},
		{
			query.QueryInput{
				Statement:  "SELECT * FROM users",
				Parameters: []string{},
			},
			[]interface{}{},
			0,
		},
		{
			query.QueryInput{
				Statement:  "SELECT * FROM users WHERE active = ?",
				Parameters: []string{"b:true"},
			},
			[]interface{}{true},
			1,
		},
		{
			query.QueryInput{
				Statement:  "SELECT * FROM users WHERE active = ? AND balance > ?",
				Parameters: []string{"b:false", "f:100.00"},
			},
			[]interface{}{false, 100.00},
			2,
		},
		{
			query.QueryInput{
				Statement: `insert into "names" ("name", "updated_at", "created_at") values (?, ?, ?)`,
				Parameters: []string{
					"s:8hB83kkILzxM1qcQtOto1CYq6fRsiZFA",
					"s:2024-07-31 13:55:01",
					"s:2024-07-31 13:55:01",
				},
			},
			[]interface{}{"8hB83kkILzxM1qcQtOto1CYq6fRsiZFA", "2024-07-31 13:55:01", "2024-07-31 13:55:01"},
			3,
		},
	}

	for _, test := range tests {
		statementLength, count, err = http.ParseQueryStatementAndParameters(test.input, &statement, &parameters)

		if err != nil {
			t.Errorf("Error parsing query statement and parameters: %v", err)
		}

		if count != test.count {
			t.Errorf("Expected %d, got %d", test.count, count)
		}

		if statementLength != len(test.input.Statement) {
			t.Errorf("Expected %d, got %d", len(test.input.Statement), statementLength)
		}

		for i := 0; i < count; i++ {
			if parameters[i] != test.output[i] {
				t.Errorf("Expected %v, got %v", test.output[i], parameters[i])
			}
		}
	}
}

func BenchmarkParseQueryStatementAndParameters(b *testing.B) {
	var statement = make([]byte, 1*1024*1024) // 1 Mib
	var parameters = make([]interface{}, 999)

	for i := 0; i < b.N; i++ {
		http.ParseQueryStatementAndParameters(
			query.QueryInput{
				Statement:  "SELECT * FROM users WHERE id = ? LIMIT ?",
				Parameters: []string{"i:100", "i:1"},
			},
			&statement,
			&parameters,
		)
	}
}
