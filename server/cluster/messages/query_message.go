package messages

import "litebase/server/sqlite3"

type QueryMessage struct {
	AccessKeyId  string
	BranchId     string
	DatabaseHash string
	DatabaseId   string
	ID           []byte
	Parameters   []sqlite3.StatementParameter
	Statement    []byte
}
