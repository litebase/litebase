package messages

type NodeMessage interface {
	Error() string
	Id() []byte
	Type() string
}
