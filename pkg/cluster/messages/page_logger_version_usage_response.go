package messages

type PageLoggerVersionUsageResponse struct {
	BranchID   string
	DatabaseID string
	Versions   []int64
}
