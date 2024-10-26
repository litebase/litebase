package messages

type ErrorMessage struct {
	ID      []byte
	Message string
}

func (e ErrorMessage) Error() string {
	return e.Message
}

func (e ErrorMessage) Id() []byte {
	return e.ID
}

func (e ErrorMessage) Type() string {
	return "ErrorMessage"
}
