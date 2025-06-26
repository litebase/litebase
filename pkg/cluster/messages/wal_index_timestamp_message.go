package messages

type WALIndexTimestampMessage struct {
	BranchID   string
	DatabaseID string
	Timestamp  int64
}
