package cluster

type ReplicationGroupReplicatedWrite struct {
	Addresses    []string
	Data         []byte
	Deadline     int64
	Key          string
	PreparedAt   int64
	Proposer     string
	ReplicatedAt int64
	SHA256       string
}
