package database

import (
	"github.com/litebase/litebase/pkg/sqlite3"
)

type QueryValidator struct {
}

func ValidateQuery(statement *sqlite3.Statement, parameters ...sqlite3.StatementParameter) error {
	var errors = map[string][]string{}

	if statement.SQL() == "" {
		errors["statement"] = append(errors["statement"], "A query statement is required")
	}

	if !numberOfParametersMatchesStatement(statement, parameters...) {
		errors["parameters"] = append(errors["parameters"], "Query parameters must match the number of placeholders")
	}

	if len(errors) > 0 {
		return NewQueryValidationError(errors)
	}

	return nil
}

func numberOfParametersMatchesStatement(statement *sqlite3.Statement, parameters ...sqlite3.StatementParameter) bool {
	return len(parameters) == statement.ParameterCount()
}
