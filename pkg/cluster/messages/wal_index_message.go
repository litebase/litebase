package messages

type WALIndexMessage struct {
	BranchID   string
	DatabaseID string
	Versions   []int64
}
