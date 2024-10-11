package node

import (
	"encoding/gob"
	"litebase/server/sqlite3"
)

type NodeMessage struct {
	Id          string
	Type        string
	Data        interface{}
	EndOfStream bool
	Error       string
}

type QueryMessage struct {
	AccessKeyId  string
	BranchId     string
	DatabaseHash string
	DatabaseId   string
	Id           string
	Parameters   []any
	Statement    string
}

type QueryMessageResponse struct {
	Changes         int64
	Columns         []string
	Latency         float64
	LastInsertRowID int64
	RowCount        int
	Rows            [][]sqlite3.Column
}

type NodeConnectionMessage struct {
	Address string
	Id      string
}

type WALReplicationWriteMessage struct {
	BranchId   string
	DatabaseId string
	Data       []byte
	Offset     int64
	Sequence   int64
	Sha256     [32]byte
	Timestamp  int64
}

type WALReplicationTruncateMessage struct {
	BranchId   string
	DatabaseId string
	Size       int64
	Sequence   int64
	Timestamp  int64
}

func registerNodeMessages() {
	gob.Register(NodeConnectionMessage{})
	gob.Register(NodeMessage{})
	gob.Register(QueryMessage{})
	gob.Register(QueryMessageResponse{})
	gob.Register(WALReplicationWriteMessage{})
	gob.Register(WALReplicationTruncateMessage{})
}
