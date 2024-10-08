package node

type NodeReplicaCheckpointer interface {
	CheckpointReplica(databaseId, branchId string, timestamp int64) error
}
