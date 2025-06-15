package messages

type TieredFileSystemReplicationMessage struct {
	Data     []byte
	Deadline int64
	Group    []string
	Leader   string
	Path     string
	Sha256   []byte
}
