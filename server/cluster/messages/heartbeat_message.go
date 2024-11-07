package messages

type HeartbeatMessage struct {
	Address string
	ID      []byte
	Time    int64
}
