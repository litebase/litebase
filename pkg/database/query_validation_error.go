package database

import (
	"encoding/json"
	"fmt"
)

type QueryValidationError struct {
	Errors map[string][]string `json:"errors"`
}

func (e *QueryValidationError) Error() string {
	jsonString, _ := json.Marshal(e.Errors)

	return fmt.Sprintf("Query Error: %s", string(jsonString))
}

func NewQueryValidationError(errors map[string][]string) *QueryValidationError {
	return &QueryValidationError{
		errors,
	}
}
