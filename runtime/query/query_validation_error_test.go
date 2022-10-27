package query_test

import (
	"litebasedb/runtime/query"
	"testing"
)

func TestQueryValidationError(t *testing.T) {
	errors := map[string][]string{
		"statement": {"A statement is required"},
	}
	err := query.NewQueryValidationError(errors)

	if err.Error() != `Query Error: {"statement":["A statement is required"]}` {
		t.Fatal("Error message is not correct:", err.Error())
	}
}
