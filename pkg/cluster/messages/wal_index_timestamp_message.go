package messages

type WALIndexTimestampMessage struct {
	BranchId   string
	DatabaseId string
	Timestamp  int64
}
