package messages

type ReplicationGroupWriteMessage struct {
	Addresses []string
	Data      []byte
	Deadline  int64
	Key       string
	Proposer  string
	SHA256    string
}
