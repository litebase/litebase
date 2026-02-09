package backups

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/storage"
)

type RollbackLogger struct {
	buffers    sync.Pool
	DatabaseID string
	BranchID   string
	dataKey    []byte   // Optional: 32-byte encryption key
	encrypted  bool     // Whether rollback logs should be encrypted
	keyHash    [32]byte // Optional: SHA256 hash of encryption key
	logs       map[int64]*RollbackLog
	mutex      *sync.Mutex
	tieredFS   *storage.FileSystem
}

func NewRollbackLogger(tieredFS *storage.FileSystem, databaseId, branchId string) *RollbackLogger {
	return &RollbackLogger{
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
		DatabaseID: databaseId,
		BranchID:   branchId,
		encrypted:  false,
		logs:       make(map[int64]*RollbackLog),
		mutex:      &sync.Mutex{},
		tieredFS:   tieredFS,
	}
}

// ConfigureEncryption sets the encryption parameters for the RollbackLogger.
// This must be called before any rollback logs are opened or created.
func (rl *RollbackLogger) ConfigureEncryption(dataKey []byte, keyHash [32]byte) error {
	if len(dataKey) != 32 {
		return fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	rl.dataKey = dataKey
	rl.keyHash = keyHash
	rl.encrypted = true

	// Configure encryption for all existing logs
	for _, rollbackLog := range rl.logs {
		rollbackLog.dataKey = dataKey
		rollbackLog.keyHash = keyHash
		rollbackLog.encrypted = true
	}

	return nil
}

func (rl *RollbackLogger) Close() error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	// Release the buffers
	rl.buffers = sync.Pool{}

	for _, l := range rl.logs {
		if err := l.Close(); err != nil {
			log.Println("Error closing rollback log", err)
		}
	}

	return nil
}

func (rl *RollbackLogger) Commit(timestamp, offset, size int64) error {
	rollbackLog, err := rl.GetLog(timestamp)

	if err != nil {
		log.Println("Error getting rollback log", err)
		return err
	}

	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	return rollbackLog.Commit(offset, size)
}

func (rl *RollbackLogger) GetLog(timestamp int64) (*RollbackLog, error) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	startOfHour := time.Unix(0, timestamp).UTC()

	startOfHourTimestamp := startOfHour.Truncate(time.Hour).UnixNano()

	if l, ok := rl.logs[startOfHourTimestamp]; ok {
		return l, nil
	}

	var rollbackLog *RollbackLog
	var err error

	if rl.encrypted && rl.dataKey != nil {
		rollbackLog, err = OpenRollbackLogWithEncryption(
			rl.tieredFS,
			rl.DatabaseID,
			rl.BranchID,
			startOfHourTimestamp,
			rl.dataKey,
			rl.keyHash,
		)
	} else {
		rollbackLog, err = OpenRollbackLog(
			rl.tieredFS,
			rl.DatabaseID,
			rl.BranchID,
			startOfHourTimestamp,
		)
	}

	if err != nil {
		log.Println("Error opening page log", err)
		return nil, err
	}

	rl.logs[startOfHourTimestamp] = rollbackLog

	return rl.logs[startOfHourTimestamp], nil
}

func (rl *RollbackLogger) Log(pageNumber, timestamp int64, data []byte) (size int64, err error) {
	compressionBuffer := rl.buffers.Get().(*bytes.Buffer)
	defer rl.buffers.Put(compressionBuffer)

	compressionBuffer.Reset()

	rollbackLog, err := rl.GetLog(timestamp)

	if err != nil {
		log.Println("Error opening page log", err)
		return 0, err
	}

	return rollbackLog.AppendLog(
		compressionBuffer,
		NewRollbackLogEntry(pageNumber, timestamp, data),
	)
}

func (rl *RollbackLogger) Rollback(timestamp, offset, size int64) error {
	rollbackLog, err := rl.GetLog(timestamp)

	if err != nil {
		log.Println("Error getting rollback log", err)
		return err
	}

	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	return rollbackLog.Rollback(offset, size)
}

func (rl *RollbackLogger) StartFrame(timestamp int64) (int64, int64, error) {
	rollbackLog, err := rl.GetLog(timestamp)

	if err != nil {
		log.Println("Error opening page log", err)
		return 0, 0, err
	}

	offset, size, err := rollbackLog.AppendFrame(timestamp)

	if err != nil {
		log.Println("Error appending frame to rollback log", err)
		return 0, 0, err
	}

	return offset, size, nil
}
