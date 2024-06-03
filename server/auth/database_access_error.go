package auth

type DatabaseAccessError struct {
	message string
}

func (e DatabaseAccessError) Error() string {
	return e.message
}

func NewDatabaseAccessError() DatabaseAccessError {
	return DatabaseAccessError{
		"Authorization Denied: You do not have access to this database.",
	}
}
