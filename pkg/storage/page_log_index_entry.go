package storage

import (
	"encoding/binary"
	"log/slog"

	"github.com/litebase/litebase/internal/utils"
)

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

	uint64PageNumber, err := utils.SafeInt64ToUint64(int64(pageLogIndexEntry.PageNumber))

	if err != nil {
		slog.Error("Error encoding page log index entry page number", "error", err)
		return nil
	}

	binary.LittleEndian.PutUint64(data[0:8], uint64PageNumber)

	uint64Version, err := utils.SafeInt64ToUint64(int64(pageLogIndexEntry.Version))

	if err != nil {
		slog.Error("Error encoding page log index entry version", "error", err)
		return nil
	}

	binary.LittleEndian.PutUint64(data[8:16], uint64Version)

	uint64Offset, err := utils.SafeInt64ToUint64(pageLogIndexEntry.Offset)

	if err != nil {
		slog.Error("Error encoding page log index entry offset", "error", err)
		return nil
	}

	binary.LittleEndian.PutUint64(data[16:24], uint64Offset)

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

	pageNumberUint64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[0:8]))

	if err != nil {
		slog.Error("Error decoding page log index entry page number", "error", err)
		return PageLogIndexEntry{}
	}

	pageNumber := PageNumber(pageNumberUint64)

	pageVersionUint64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[8:16]))

	if err != nil {
		slog.Error("Error decoding page log index entry version", "error", err)
		return PageLogIndexEntry{}
	}

	version := PageVersion(pageVersionUint64)

	offset, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[16:24]))

	if err != nil {
		slog.Error("Error decoding page log index entry offset", "error", err)
		return PageLogIndexEntry{}
	}

	tombstoned := data[24] == 1

	return PageLogIndexEntry{
		Offset:     offset,
		PageNumber: pageNumber,
		Tombstoned: tombstoned,
		Version:    version,
	}
}
