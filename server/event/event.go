package event

type Event struct {
	Body         map[string]interface{} `json:"body"`
	BranchUuid   string                 `json:"branch_id"`
	DatabaseUuid string                 `json:"database_id"`
	Host         string                 `json:"host"`
	Method       string                 `json:"method"`
	Path         string                 `json:"path"`
	Server       map[string]string      `json:"server"`
}
