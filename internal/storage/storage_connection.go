package storage

type StorageRequest struct {
	BranchUuid   string
	Command      string
	Data         []byte
	DatabaseUuid string
	Id           string
	Key          string
	Page         int64
	Size         int64
}

type StorageResponse struct {
	Error string
	Data  []byte
	Id    string
}
