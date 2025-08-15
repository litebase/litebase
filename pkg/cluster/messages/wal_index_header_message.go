package messages

type WALIndexHeaderMessage struct {
	BranchID       string
	DatabaseID     string
	DatabaseHash   string
	NodeHash       string
	Timestamp      int64
	WALIndexHeader []byte
}
