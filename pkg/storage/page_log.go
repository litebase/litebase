package storage

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/litebase/litebase/internal/storage"
)

const (
	PageSize = 4096
)

type PageLog struct {
	compactedAt time.Time
	deleted     bool
	fileSystem  *FileSystem
	file        storage.File
	index       *PageLogIndex
	mutex       *sync.Mutex
	Path        string
}

func NewPageLog(fileSystem *FileSystem, path string) (*PageLog, error) {
	pl := &PageLog{
		fileSystem: fileSystem,
		mutex:      &sync.Mutex{},
		Path:       path,
	}

	var pli *PageLogIndex
	var err error

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		pli = NewPageLogIndex(fileSystem, fmt.Sprintf("%s_INDEX", path))
	}()

	go func() {
		defer wg.Done()

		err = pl.openFile()

	}()
	wg.Wait()

	if err != nil {
		return nil, err
	}

	pl.index = pli

	return pl, nil
}

func (pl *PageLog) Append(page int64, version int64, value []byte) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	offset, err := pl.File().Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	if len(value) != PageSize {
		return errors.New("value size is less than page size")
	}

	_, err = pl.File().Write(value)

	if err != nil {
		return err
	}

	err = pl.index.Put(PageNumber(page), PageVersion(version), offset, value)

	if err != nil {
		return err
	}

	return nil
}

// Execute the close logic without locking the mutex.
func (pl *PageLog) close() error {
	if pl.file != nil {
		defer func() {
			pl.file = nil
		}()

		return pl.file.Close()
	}

	if pl.index != nil {
		err := pl.index.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

// Close the page log.
func (pl *PageLog) Close() error {
	// pl.mutex.Lock()
	// defer pl.mutex.Unlock()

	return pl.close()
}

// Compact the page log contents into the durable file system.
func (pl *PageLog) compact(durableFileSystem *DurableDatabaseFileSystem) error {
	// TODO: The page log needs to be durably marked as compacted to avoid
	// overwrites. This also will allow us to retry compaction if it fails due
	// to a crash or other error.
	if !pl.compactedAt.IsZero() {
		return nil
	}

	if pl.deleted {
		panic("PageLog is deleted")
	}

	// Get the latest version of each page in the log.
	latestVersions := pl.index.getLatestPageVersions()
	data := make([]byte, PageSize)

	for _, entry := range latestVersions {
		found, _, err := pl.Get(entry.PageNumber, entry.Version, data)

		if err != nil {
			return err
		}

		if found {
			err := durableFileSystem.WriteToRange(int64(entry.PageNumber), data)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Close and delete the PageLog file.
func (pl *PageLog) Delete() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	err := pl.close()

	if err != nil {
		return err
	}

	pl.deleted = true
	pl.index.Delete()
	pl.index = nil

	return pl.fileSystem.Remove(pl.Path)
}

// Return the file of the PageLog.
func (pl *PageLog) File() storage.File {
	if pl.deleted {
		return nil
	}

	if pl.file == nil {
		err := pl.openFile()

		if err != nil {
			log.Println("Error opening page log:", err)
			return nil
		}
	}

	return pl.file
}

// Get a page from the PageLog by page number and version.
func (pl *PageLog) Get(page PageNumber, version PageVersion, data []byte) (bool, PageVersion, error) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	found, foundVersion, offset, err := pl.index.Find(page, version)

	if err != nil {
		return false, 0, err
	}

	if !found {
		return false, 0, nil
	}

	_, err = pl.File().Seek(offset, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to offset", offset, err)
		return false, 0, err
	}

	_, err = pl.File().Read(data)

	if err != nil {
		log.Println("Error reading page data", err)
		return false, 0, err
	}

	return true, foundVersion, nil
}

func (pl *PageLog) openFile() error {
	var err error

tryOpen:
	pl.file, err = pl.fileSystem.OpenFileDirect(pl.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = pl.fileSystem.MkdirAll(filepath.Dir(pl.Path), 0755)

			if err != nil {
				return err
			}

			goto tryOpen
		}

		return err
	}

	return nil
}

// Mark all pages of a specific version as tombstoned.
func (pl *PageLog) Tombstone(version PageVersion) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pages := pl.index.findPagesByVersion(version)

	for _, pageNumber := range pages {
		err := pl.index.Tombstone(PageNumber(pageNumber), PageVersion(version))

		if err != nil {
			return err
		}
	}

	return nil
}
