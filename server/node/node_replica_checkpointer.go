package node

type NodeReplicaCheckpointer interface {
	CheckpointReplica(databaseUuid, branchUuid string, timestamp int64) error
}
