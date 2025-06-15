package storage

type StorageNode interface {
	IsPrimary() bool
	IsReplica() bool
}
