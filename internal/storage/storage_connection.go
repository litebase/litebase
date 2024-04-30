package storage

type StorageRequest struct {
	BranchUuid   string
	Command      string
	Data         []byte
	DatabaseUuid string
	Key          string
	Page         int64
}

type StorageResponse struct {
	Error string
	Data  []byte
}
