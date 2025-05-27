package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/litebase/litebase/internal/storage"
)

type PageLogIndex struct {
	file       storage.File
	fileSystem *FileSystem
	memory     map[PageNumber][]PageLogIndexEntry
	path       string
}

func NewPageLogIndex(fileSystem *FileSystem, path string) *PageLogIndex {
	pli := &PageLogIndex{
		fileSystem: fileSystem,
		path:       path,
		memory:     make(map[PageNumber][]PageLogIndexEntry),
	}

	pli.load()

	return pli
}

func (pli *PageLogIndex) Close() error {
	if pli.file != nil {
		defer func() {
			pli.file = nil
		}()

		return pli.file.Close()
	}

	return nil
}

func (pli *PageLogIndex) Delete() error {
	if pli.file != nil {
		defer func() {
			pli.file = nil
		}()

		return pli.fileSystem.Remove(pli.path)
	}

	return nil
}

func (pli *PageLogIndex) Empty() bool {
	return len(pli.memory) == 0
}

// Get the latest version of each page.
func (pli *PageLogIndex) getLatestPageVersions() map[PageNumber]PageLogIndexEntry {
	latestVersions := make(map[PageNumber]PageLogIndexEntry)

	for pageNumber, entries := range pli.memory {
		latestPageVersion := PageVersion(0)
		index := -1

		for i, entry := range entries {
			if entry.Version > latestPageVersion {
				latestPageVersion = entry.Version
				index = i
			}
		}

		if latestPageVersion > 0 {
			latestVersions[pageNumber] = entries[index]
		}
	}

	return latestVersions
}

func (pli *PageLogIndex) File() storage.File {
	if pli.file == nil {
		var err error
	tryOpen:
		pli.file, err = pli.fileSystem.OpenFileDirect(pli.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

		if err != nil {
			if os.IsNotExist(err) {
				err = pli.fileSystem.MkdirAll(filepath.Dir(pli.path), 0755)

				if err != nil {
					return nil
				}

				goto tryOpen
			}

			return nil
		}
	}

	return pli.file
}

func (pli *PageLogIndex) Find(page PageNumber, version PageVersion) (bool, PageVersion, int64, error) {
	if entries, ok := pli.memory[page]; ok {
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Version <= version || version == 0 {
				if entries[i].Tombstoned {
					continue
				}

				return true, entries[i].Version, int64(entries[i].Offset), nil
			}
		}
	}

	return false, 0, 0, nil
}

func (pli *PageLogIndex) findPagesByVersion(pageVersion PageVersion) []PageNumber {
	pages := make([]PageNumber, 0)

	for pageNumber, entries := range pli.memory {
		for _, entry := range entries {
			if entry.Version == pageVersion && !entry.Tombstoned {
				pages = append(pages, pageNumber)
			}
		}
	}

	return pages
}

func (pli *PageLogIndex) load() error {
	pli.memory = make(map[PageNumber][]PageLogIndexEntry)

	stat, err := pli.File().Stat()

	if err != nil {
		return err
	}

	size := stat.Size()
	indexData := make([]byte, size)

	_, err = pli.File().ReadAt(indexData, 0)

	if err != nil {
		return err
	}

	for i := 0; i < len(indexData); i += PageLogIndexEntryLength {
		if i+PageLogIndexEntryLength > len(indexData) {
			break
		}

		entry := DecodePageLogIndexEntry(indexData[i : i+PageLogIndexEntryLength])

		if entry == (PageLogIndexEntry{}) {
			continue
		}

		// Check if an existing entry exists with the same page number and version
		if entry.Tombstoned {
			if entries, ok := pli.memory[entry.PageNumber]; ok {
				for j, e := range entries {
					if e.Version == entry.Version {
						if entry.Tombstoned {
							pli.memory[entry.PageNumber][j].Tombstoned = true
						}

						break
					}
				}
			}
		} else {
			pli.memory[entry.PageNumber] = append(pli.memory[entry.PageNumber], entry)
		}
	}

	return nil
}

func (pli *PageLogIndex) Put(pageNumber PageNumber, versionNumber PageVersion, offset int64, value []byte) error {
	entry := NewPageLogIndexEntry(
		pageNumber,
		versionNumber,
		offset,
		false,
	)

	_, err := pli.File().Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	_, err = pli.File().Write(entry.Encode())

	if err != nil {
		return err
	}

	pli.memory[pageNumber] = append(
		pli.memory[pageNumber],
		PageLogIndexEntry{
			Offset:     offset,
			PageNumber: pageNumber,
			Version:    versionNumber,
		},
	)

	return nil
}

func (pli *PageLogIndex) Tombstone(pageNumber PageNumber, versionNumber PageVersion) error {
	if entries, ok := pli.memory[pageNumber]; ok {
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Version == versionNumber {
				entries[i].Tombstoned = true
			}
		}
	}

	entry := NewPageLogIndexEntry(
		pageNumber,
		versionNumber,
		0,
		true,
	)

	_, err := pli.File().Write(entry.Encode())

	if err != nil {
		return err
	}

	return nil
}
