package messages

type WALReplicationTruncateMessage struct {
	BranchId   string
	DatabaseId string
	ID         []byte
	Size       int64
	Sequence   int64
	Timestamp  int64
}
