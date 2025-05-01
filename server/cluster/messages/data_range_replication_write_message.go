package messages

type RangeReplicationWriteMessage struct {
	BranchId   string
	DatabaseId string
	Data       []byte
	ID         []byte
	Offset     int64
	Sha256     [32]byte
	Size       int64
	Sequence   int64
	Timestamp  int64
}
