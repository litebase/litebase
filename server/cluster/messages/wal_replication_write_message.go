package messages

type WALReplicationWriteMessage struct {
	BranchId   string
	DatabaseId string
	Data       []byte
	ID         []byte
	Offset     int64
	Sequence   int64
	Sha256     [32]byte
	Timestamp  int64
}

func (w WALReplicationWriteMessage) Error() string {
	return ""
}

func (w WALReplicationWriteMessage) Id() []byte {
	return w.ID
}

func (w WALReplicationWriteMessage) Type() string {
	return "WALReplicationWriteMessage"
}
