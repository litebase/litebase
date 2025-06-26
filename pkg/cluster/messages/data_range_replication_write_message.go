package messages

type RangeReplicationWriteMessage struct {
	BranchID   string
	DatabaseID string
	Data       []byte
	ID         []byte
	Offset     int64
	Sha256     [32]byte
	Size       int64
	Sequence   int64
	Timestamp  int64
}
