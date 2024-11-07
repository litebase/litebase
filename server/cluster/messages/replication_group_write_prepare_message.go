package messages

type ReplicationGroupWritePrepareMessage struct {
	Addresses []string
	Key       string
	Proposer  string
	SHA256    string
}
