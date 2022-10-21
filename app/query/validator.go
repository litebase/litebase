package query

import (
	"litebasedb/runtime/app/sqlite3"
)

type QueryValidator struct {
}

func ValidateQuery(batch []*Query, statement *sqlite3.Statement, parameters ...interface{}) error {
	var errors = map[string][]string{}

	if len(batch) > 0 {
		return nil
	}

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

func numberOfParametersMatchesStatement(statement *sqlite3.Statement, parameters ...interface{}) bool {
	return len(parameters) == statement.ParameterCount()
}
