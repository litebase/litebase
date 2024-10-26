package messages

type WALReplicationTruncateMessage struct {
	BranchId   string
	DatabaseId string
	ID         []byte
	Size       int64
	Sequence   int64
	Timestamp  int64
}

func (w WALReplicationTruncateMessage) Error() string {
	return ""
}

func (w WALReplicationTruncateMessage) Id() []byte {
	return w.ID
}

func (w WALReplicationTruncateMessage) Type() string {
	return "WALReplicationTruncateMessage"
}
