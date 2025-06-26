package messages

type RangeReplicationTruncateMessage struct {
	BranchID   string
	DatabaseID string
	ID         []byte
	Size       int64
	Sequence   int64
	Timestamp  int64
}
