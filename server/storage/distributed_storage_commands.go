package storage

type DistributedStorageCommand int

const (
	ConnectionStorageCommand   DistributedStorageCommand = 0x01
	CloseStorageCommand        DistributedStorageCommand = 0x02
	CreateStorageCommand       DistributedStorageCommand = 0x03
	MkdirStorageCommand        DistributedStorageCommand = 0x04
	MkdirAllStorageCommand     DistributedStorageCommand = 0x05
	OpenStorageCommand         DistributedStorageCommand = 0x06
	OpenFileStorageCommand     DistributedStorageCommand = 0x07
	ReadStorageCommand         DistributedStorageCommand = 0x08
	ReadAtStorageCommand       DistributedStorageCommand = 0x09
	ReadDirStorageCommand      DistributedStorageCommand = 0x0A
	ReaddirStorageCommand      DistributedStorageCommand = 0x0B
	ReadFileStorageCommand     DistributedStorageCommand = 0x0C
	RemoveStorageCommand       DistributedStorageCommand = 0x0D
	RemoveAllStorageCommand    DistributedStorageCommand = 0x0E
	RenameStorageCommand       DistributedStorageCommand = 0x0F
	SeekStorageCommand         DistributedStorageCommand = 0x10
	StatStorageCommand         DistributedStorageCommand = 0x11
	StatFileStorageCommand     DistributedStorageCommand = 0x12
	SyncStorageCommand         DistributedStorageCommand = 0x13
	TruncateStorageCommand     DistributedStorageCommand = 0x14
	TruncateFileStorageCommand DistributedStorageCommand = 0x15
	WriteStorageCommand        DistributedStorageCommand = 0x16
	WriteAtStorageCommand      DistributedStorageCommand = 0x17
	WriteFileStorageCommand    DistributedStorageCommand = 0x18
	WriteStringStorageCommand  DistributedStorageCommand = 0x19
	WriteToStorageCommand      DistributedStorageCommand = 0x1A
)

func (dsc DistributedStorageCommand) String() string {
	switch dsc {
	case ConnectionStorageCommand:
		return "ConnectionStorageCommand"
	case CloseStorageCommand:
		return "CloseStorageCommand"
	case CreateStorageCommand:
		return "CreateStorageCommand"
	case MkdirStorageCommand:
		return "MkdirStorageCommand"
	case MkdirAllStorageCommand:
		return "MkdirAllStorageCommand"
	case OpenStorageCommand:
		return "OpenStorageCommand"
	case OpenFileStorageCommand:
		return "OpenFileStorageCommand"
	case ReadStorageCommand:
		return "ReadStorageCommand"
	case ReadAtStorageCommand:
		return "ReadAtStorageCommand"
	case ReadDirStorageCommand:
		return "ReadDirStorageCommand"
	case ReaddirStorageCommand:
		return "ReaddirStorageCommand"
	case ReadFileStorageCommand:
		return "ReadFileStorageCommand"
	case RemoveStorageCommand:
		return "RemoveStorageCommand"
	case RemoveAllStorageCommand:
		return "RemoveAllStorageCommand"
	case RenameStorageCommand:
		return "RenameStorageCommand"
	case SeekStorageCommand:
		return "SeekStorageCommand"
	case StatStorageCommand:
		return "StatStorageCommand"
	case StatFileStorageCommand:
		return "StatFileStorageCommand"
	case SyncStorageCommand:
		return "SyncStorageCommand"
	case TruncateStorageCommand:
		return "TruncateStorageCommand"
	case TruncateFileStorageCommand:
		return "TruncateFileStorageCommand"
	case WriteStorageCommand:
		return "WriteStorageCommand"
	case WriteAtStorageCommand:
		return "WriteAtStorageCommand"
	case WriteFileStorageCommand:
		return "WriteFileStorageCommand"
	case WriteStringStorageCommand:
		return "WriteStringStorageCommand"
	case WriteToStorageCommand:
		return "WriteToStorageCommand"
	}

	return "Unknown"
}
