package storage

type StorageReplicationGroup interface {
	Commit(key string, sha256Hash string) error
	Prepare(key string, sha256Hash string) error
	Write(key string, data []byte) error
}
