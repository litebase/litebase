package messages

type WALIndexMessage struct {
	BranchId   string
	DatabaseId string
	Versions   []int64
}
