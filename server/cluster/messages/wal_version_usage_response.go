package messages

type WALVersionUsageResponse struct {
	BranchId   string
	DatabaseId string
	Versions   []int64
}
