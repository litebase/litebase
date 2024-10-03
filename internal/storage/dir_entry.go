package storage

/*
DirEntry represents a directory entry.

When encoded the entry will have the following binary format:

| Offset | Length | Description                                |
|--------|--------|--------------------------------------------|
| 0      | 1      | 1 if the entry is a directory, 0 otherwise |
| 1      | n      | The name of the entry                      |
*/
type DirEntry struct {
	Name  string
	IsDir bool
}

func DecodeDirEntry(data []byte) DirEntry {
	return DirEntry{
		IsDir: data[0] == 1,
		Name:  string(data[1:]),
	}
}

func (de DirEntry) Encode() []byte {
	data := make([]byte, 1+len(de.Name))

	if de.IsDir {
		data[0] = 1
	} else {
		data[0] = 0
	}

	copy(data[1:], de.Name)

	return data
}
