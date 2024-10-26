package messages

type HeartbeatMessage struct {
	ID []byte
}

func (h HeartbeatMessage) Error() string {
	return ""
}

func (h HeartbeatMessage) Id() []byte {
	return h.ID
}

func (h HeartbeatMessage) Type() string {
	return "HeartbeatMessage"
}
