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

type NodeGossipMessage struct {
	Action  string
	Address string
	Group   string
}

type QueryMessage struct {
	AccessKeyId  string
	BranchUuid   string
	DatabaseHash string
	DatabaseUuid string
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

type ReplicaConnection struct {
	Address string
	Id      string
}

type ReplicaConnectionResponse struct {
	Id string
}

type WALMessage struct {
	BranchUuid   string
	DatabaseUuid string
}

type WALMessageResponse struct {
	BranchUuid   string
	ChunkNumber  int
	Data         []byte
	DatabaseUuid string
	LastChunk    bool
	Sha256       [32]byte
	Timestamp    int64
	TotalChunks  int
}

type WALCheckpointMessage struct {
	BranchUuid   string
	DatabaseUuid string
	Timestamp    int64
}

type WALReplicationMessage struct {
	BranchUuid   string
	DatabaseUuid string
	Data         []byte
	Offset       int
	Length       int
	Sha256       [32]byte
	Timestamp    int64
}

func registerNodeMessages() {
	gob.Register(NodeMessage{})
	gob.Register(QueryMessage{})
	gob.Register(QueryMessageResponse{})
	gob.Register(ReplicaConnection{})
	gob.Register(ReplicaConnectionResponse{})
	gob.Register(WALMessage{})
	gob.Register(WALMessageResponse{})
	gob.Register(WALCheckpointMessage{})
	gob.Register(WALReplicationMessage{})
}
