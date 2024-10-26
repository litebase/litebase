package messages

type NodeConnectionMessage struct {
	Address string
	ID      []byte
}

func (n NodeConnectionMessage) Error() string {
	return ""
}

func (n NodeConnectionMessage) Id() []byte {
	return n.ID
}

func (n NodeConnectionMessage) Type() string {
	return "NodeConnectionMessage"
}
