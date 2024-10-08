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

type ReplicaConnection struct {
	Address string
	Id      string
}

type ReplicaConnectionResponse struct {
	Id string
}

type WALMessage struct {
	BranchId   string
	DatabaseId string
}

type WALMessageResponse struct {
	BranchId    string
	ChunkNumber int
	Data        []byte
	DatabaseId  string
	LastChunk   bool
	Sha256      [32]byte
	Timestamp   int64
	TotalChunks int
}

type WALCheckpointMessage struct {
	BranchId   string
	DatabaseId string
	Timestamp  int64
}

type WALReplicationMessage struct {
	BranchId   string
	DatabaseId string
	Data       []byte
	Offset     int
	Length     int
	Sha256     [32]byte
	Timestamp  int64
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
