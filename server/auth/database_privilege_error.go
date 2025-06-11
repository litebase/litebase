package auth

import (
	"fmt"
)

type DatabasePrivilegeError struct {
	message string
}

func (e DatabasePrivilegeError) Error() string {
	return e.message
}

func NewDatabasePrivilegeError(privilege Privilege) DatabasePrivilegeError {
	return DatabasePrivilegeError{
		fmt.Sprintf("'Authorization Denied: The %s privilege is required to perform this query.", privilege),
	}
}
