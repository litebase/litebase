package messages

type ReplicationGroupWriteCommitMessage struct {
	Addresses []string
	Key       string
	Proposer  string
	SHA256    string
}
