package database

type DatabaseKey struct {
	ID               int64  `json:"-"`
	DatabaseID       string `json:"database_id"`
	DatabaseBranchID string `json:"database_branch_id"`
	Key              string `json:"key"`
}
