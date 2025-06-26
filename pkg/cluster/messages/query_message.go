package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessage struct {
	AccessKeyID string
	BranchID    string
	DatabaseKey string
	DatabaseID  string
	ID          string
	Parameters  []sqlite3.StatementParameter
	Statement   string
}
