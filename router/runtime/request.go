package runtime

type RuntimeRequest struct {
	BranchUuid   string                 `json:"branchUuid"`
	DatabaseUuid string                 `json:"databaseUuid"`
	Body         map[string]interface{} `json:"body"`
	Method       string                 `json:"method"`
	Path         string                 `json:"path"`
	Server       map[string]interface{} `json:"server"`
}
