package database_test

import (
	"litebase/server/database"
	"testing"
)

func TestQueryValidationError(t *testing.T) {
	errors := map[string][]string{
		"statement": {"A statement is required"},
	}
	err := database.NewQueryValidationError(errors)

	if err.Error() != `Query Error: {"statement":["A statement is required"]}` {
		t.Fatal("Error message is not correct:", err.Error())
	}
}
