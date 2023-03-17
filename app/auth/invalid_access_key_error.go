package auth

import "fmt"

type InvalidAccessKeyError struct {
	message string
}

func (e *InvalidAccessKeyError) Error() string {
	return e.message
}

func NewInvalidAccessKeyError(message string) *InvalidAccessKeyError {
	return &InvalidAccessKeyError{
		fmt.Sprintf("'Authorization Denied: The %s privilege is required to perform this query.", message),
	}
}
