package database

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

type DatabaseWAL struct {
	BranchId       string
	DatabaseId     string
	checkpointedAt time.Time
	checkpointing  bool
	file           internalStorage.File
	fileSystem     *storage.FileSystem
	hash           string
	lastKnownSize  int64
	mutext         *sync.RWMutex
	node           *cluster.Node
	Path           string
	timestamp      int64
	walManager     *DatabaseWALManager
}

func NewDatabaseWAL(
	node *cluster.Node,
	connectionManager *ConnectionManager,
	databaseId string,
	branchId string,
	fileSystem *storage.FileSystem,
	walManager *DatabaseWALManager,
	timestamp int64,
) *DatabaseWAL {
	return &DatabaseWAL{
		BranchId:      branchId,
		DatabaseId:    databaseId,
		fileSystem:    fileSystem,
		lastKnownSize: -1,
		mutext:        &sync.RWMutex{},
		node:          node,
		Path:          fmt.Sprintf("%slogs/wal/WAL_%d", file.GetDatabaseFileBaseDir(databaseId, branchId), timestamp),
		timestamp:     timestamp,
		walManager:    walManager,
	}
}

func (wal *DatabaseWAL) Checkpointing() bool {
	return wal.checkpointing
}

func (wal *DatabaseWAL) Close() error {
	if wal.file != nil {
		err := wal.file.Close()

		if err != nil {
			log.Println(err)
		}

		wal.file = nil
	}

	return nil
}

func (wal *DatabaseWAL) Delete() error {
	wal.mutext.Lock()
	defer wal.mutext.Unlock()

	if wal.node.IsReplica() {
		return errors.New("cannot delete WAL file on replica node")
	}

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return err
	}

	err = file.Close()

	if err != nil {
		log.Println(err)
		return err
	}

	err = wal.fileSystem.Remove(wal.Path)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (wal *DatabaseWAL) File() (internalStorage.File, error) {
	if wal.file != nil {
		return wal.file, nil
	}

tryOpen:
	file, err := wal.fileSystem.OpenFile(
		wal.Path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		if os.IsNotExist(err) {
			err = wal.fileSystem.MkdirAll(filepath.Dir(wal.Path), 0755)

			if err != nil {
				return nil, err
			}
			log.Println("Creating WAL file", wal.Path)
			goto tryOpen
		} else {
			return nil, err
		}
	}

	wal.file = file

	return wal.file, nil
}

func (wal *DatabaseWAL) Hash() string {
	if wal.hash != "" {
		return wal.hash
	}

	checksum := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%d", wal.DatabaseId, wal.BranchId, wal.Timestamp()))
	wal.hash = hex.EncodeToString(checksum[:])

	return wal.hash
}

func (wal *DatabaseWAL) IsCheckpointed() bool {
	wal.mutext.RLock()
	defer wal.mutext.RUnlock()

	return !wal.checkpointedAt.IsZero()
}

// func (wal *DatabaseWAL) IsLatestVersion() bool {
// 	if wal.walManager.latestWALVersion == nil {
// 		return false
// 	}

// 	return wal.walManager.latestWALVersion.timestamp == wal.timestamp
// }

func (wal *DatabaseWAL) MarkCheckpointed() {
	wal.mutext.Lock()
	defer wal.mutext.Unlock()

	wal.checkpointing = false
	wal.checkpointedAt = time.Now()
}

func (wal *DatabaseWAL) ReadAt(p []byte, off int64) (n int, err error) {
	wal.mutext.Lock()
	defer wal.mutext.Unlock()

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	if wal.node.IsPrimary() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	if wal.node.IsReplica() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	if wal.node.IsReplica() {
		// panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	return file.ReadAt(p, off)
}

func (wal *DatabaseWAL) RequiresCheckpoint() bool {
	if wal.lastKnownSize < 0 {
		wal.Size()
	}

	return wal.checkpointedAt.IsZero() && wal.lastKnownSize > 0
}

// func (wal *DatabaseWAL) SetWALIndexHeader(header []byte) error {
// 	return wal.index.SetHeader(header)
// }

func (wal *DatabaseWAL) SetCheckpointing(checkpointing bool) error {
	if wal.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	wal.checkpointing = checkpointing

	// log.Println("checkpointing", wal.timestamp, checkpointing)

	return nil
}

func (wal *DatabaseWAL) Size() (int64, error) {
	file, err := wal.File()

	if err != nil {
		log.Println(err)

		return 0, err
	}

	info, err := file.Stat()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	size := info.Size()

	wal.lastKnownSize = size

	return size, nil
}

func (wal *DatabaseWAL) Timestamp() int64 {
	return wal.timestamp
}

// This operation is a no-op. WAL version data is immutable.
func (wal *DatabaseWAL) Truncate(size int64) error {
	wal.mutext.Lock()
	defer wal.mutext.Unlock()

	if wal.node.IsReplica() {
		return errors.New("cannot truncate WAL file on replica node")
	}

	return nil
}

func (wal *DatabaseWAL) WriteAt(p []byte, off int64) (n int, err error) {
	if wal.node.IsReplica() {
		return 0, errors.New("cannot write to WAL file on replica node")
	}

	wal.mutext.Lock()
	defer wal.mutext.Unlock()

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	n, err = file.WriteAt(p, off)

	if err != nil {
		log.Println(err)
		return n, err
	}

	return n, err
}
