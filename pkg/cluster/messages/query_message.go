package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessage struct {
	AccessKeyId string
	BranchId    string
	DatabaseKey string
	DatabaseId  string
	ID          string
	Parameters  []sqlite3.StatementParameter
	Statement   string
}
