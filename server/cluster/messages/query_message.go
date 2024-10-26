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

func (q QueryMessage) Error() string {
	return ""
}

func (q QueryMessage) Id() []byte {
	return q.ID
}

func (q QueryMessage) Type() string {
	return "QueryMessage"
}
