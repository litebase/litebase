package messages

type WALVersionUsageResponse struct {
	BranchID   string
	DatabaseID string
	Versions   []int64
}
