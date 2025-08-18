package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessage struct {
	BranchID         string
	BranchName       string
	CredentialID     string
	CredentialScheme string
	DatabaseID       string
	DatabaseName     string
	ID               string
	Parameters       []sqlite3.StatementParameter
	Statement        string
}
