package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/file"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

type WALIndex struct {
	BranchId    string
	CreatedAt   time.Time
	DatabaseId  string
	file        internalStorage.File
	fileSystem  *FileSystem
	mutex       *sync.Mutex
	TruncatedAt time.Time
	versions    []int64
}

// Create a new instance of a WAL Index
func NewWALIndex(databaseId, branchId string, fileSystem *FileSystem) *WALIndex {
	w := &WALIndex{
		BranchId:   branchId,
		DatabaseId: databaseId,
		fileSystem: fileSystem,
		mutex:      &sync.Mutex{},
		versions:   make([]int64, 0),
	}

	// Load the WAL index from the file system
	err := w.load()

	if err != nil {
		log.Println("Error loading WAL index", err)
	}

	return w
}

// Close the WAL index
func (w *WALIndex) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.file == nil {
		return nil
	}

	return w.file.Close()
}

// Get the file for the WAL index
func (w *WALIndex) File() (internalStorage.File, error) {
	if w.file != nil {
		return w.file, nil
	}

	path := fmt.Sprintf("%slogs/wal/WAL_INDEX", file.GetDatabaseFileBaseDir(w.DatabaseId, w.BranchId))

tryOpen:
	file, err := w.fileSystem.OpenFileDirect(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		if os.IsNotExist(err) {
			// Try to create the directory
			err = w.fileSystem.MkdirAll(filepath.Dir(path), 0755)

			if err != nil {
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	w.file = file

	return w.file, nil
}

// Retrieve the closest version number to the provided version number.
func (w *WALIndex) GetClosestVersion(version int64) int64 {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	slices.Sort(w.versions)

	for i := len(w.versions) - 1; i >= 0; i-- {
		v := w.versions[i]

		if v <= version {
			return v
		}
	}

	return 0
}

// Get all the versions in the WAL index
func (w *WALIndex) GetVersions() ([]int64, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	var versions = make([]int64, len(w.versions))

	copy(versions, w.versions)

	return versions, nil
}

// Load the WAL index from the file system.
func (w *WALIndex) load() error {
	file, err := w.File()

	if err != nil {
		return err
	}

	size := int64(0)

	if file != nil {
		info, err := file.Stat()

		if err != nil {
			return err
		}

		size = info.Size()
	}

	data := make([]byte, size)

	_, err = file.ReadAt(data, 0)

	if err != nil && err != io.EOF {
		return err
	}

	versions := make([]int64, 0)
	index := 0

	for index < len(data) {
		version := int64(binary.LittleEndian.Uint64(data[index : index+8]))

		if version == 0 {
			break
		}

		versions = append(versions, version)
		index += 8
	}

	slices.Sort(w.versions)

	w.versions = versions

	return nil
}

// Pesists the index to the file system.
func (w *WALIndex) persist() error {
	file, err := w.File()

	if err != nil {
		return err
	}

	// Truncate the file to remove any previous content
	err = file.Truncate(0)

	if err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	data := make([]byte, 0)
	versionBytes := make([]byte, 8)

	for _, version := range w.versions {
		binary.LittleEndian.PutUint64(versionBytes, uint64(version))
		data = append(data, versionBytes...)
	}

	_, err = file.Write(data)

	if err != nil {
		return err
	}

	return nil
}

func (w *WALIndex) RemoveVersionsFrom(timestamp int64) ([]int64, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	versions := make([]int64, 0)
	removed := make([]int64, 0)

	for _, version := range w.versions {
		if version > timestamp {
			versions = append(versions, version)
		} else {
			removed = append(removed, version)
		}
	}

	slices.Sort(versions)

	w.versions = versions

	return removed, w.persist()
}

func (w *WALIndex) SetVersions(versions []int64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	slices.Sort(versions)

	w.versions = versions

	return w.persist()
}

func (w *WALIndex) Truncate() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.TruncatedAt = time.Now().UTC()

	// Remove all versions that are older than 24 hours from the truncated time.
	for i := len(w.versions) - 1; i >= 0; i-- {
		if time.Unix(0, w.versions[i]).UTC().Before(w.TruncatedAt.Add(-24 * time.Hour)) {
			w.versions = slices.Delete(w.versions, i, i+1)
		}
	}

	slices.Sort(w.versions)

	w.persist()

	return nil
}
