package storage

import "encoding/binary"

const PageLogIndexEntryLength = 25

type PageLogIndexEntry struct {
	Offset     int64
	PageNumber PageNumber
	Tombstoned bool
	Version    PageVersion
}

func NewPageLogIndexEntry(
	pageNumber PageNumber,
	version PageVersion,
	offset int64,
	tombstoned bool,
) PageLogIndexEntry {
	return PageLogIndexEntry{
		Offset:     offset,
		PageNumber: pageNumber,
		Tombstoned: tombstoned,
		Version:    version,
	}
}

func (pageLogIndexEntry *PageLogIndexEntry) Encode() []byte {
	data := make([]byte, PageLogIndexEntryLength)

	binary.LittleEndian.PutUint64(data[0:8], uint64(pageLogIndexEntry.PageNumber))
	binary.LittleEndian.PutUint64(data[8:16], uint64(pageLogIndexEntry.Version))
	binary.LittleEndian.PutUint64(data[16:24], uint64(pageLogIndexEntry.Offset))

	if pageLogIndexEntry.Tombstoned {
		data[24] = 1
	} else {
		data[24] = 0
	}

	return data
}

func DecodePageLogIndexEntry(data []byte) PageLogIndexEntry {
	if len(data) < PageLogIndexEntryLength {
		return PageLogIndexEntry{}
	}

	pageNumber := PageNumber(binary.LittleEndian.Uint64(data[0:8]))
	version := PageVersion(binary.LittleEndian.Uint64(data[8:16]))
	offset := int64(binary.LittleEndian.Uint64(data[16:24]))
	tombstoned := data[24] == 1

	return PageLogIndexEntry{
		Offset:     offset,
		PageNumber: pageNumber,
		Tombstoned: tombstoned,
		Version:    version,
	}
}
