package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessage struct {
	AccessKeyID  string
	BranchID     string
	BranchName   string
	DatabaseID   string
	DatabaseName string
	ID           string
	Parameters   []sqlite3.StatementParameter
	Statement    string
}
