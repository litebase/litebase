package messages

type WALIndexHeaderMessage struct {
	BranchID   string
	DatabaseID string
	Header     []byte
}
