package messages

type WALIndexHeaderMessage struct {
	BranchId   string
	DatabaseId string
	Header     []byte
}
