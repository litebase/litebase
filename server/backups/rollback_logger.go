package backups

import (
	"bytes"
	"log"
	"sync"
	"time"
)

type RollbackLogger struct {
	buffers      sync.Pool
	DatabaseUuid string
	BranchUuid   string
	logs         map[int64]*RollbackLog
	mutex        *sync.Mutex
}

func NewRollbackLogger(databaseUuid, branchUuid string) *RollbackLogger {
	return &RollbackLogger{
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		logs:         make(map[int64]*RollbackLog),
		mutex:        &sync.Mutex{},
	}
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

	startOfHour := time.Unix(int64(timestamp), 0)
	startOfHourTimestamp := startOfHour.Truncate(time.Hour).Unix()

	if log, ok := rl.logs[startOfHourTimestamp]; ok {
		return log, nil
	}

	rollbackLog, err := OpenRollbackLog(rl.DatabaseUuid, rl.BranchUuid, startOfHourTimestamp)

	if err != nil {
		log.Println("Error opening page log", err)
		return nil, err
	}

	rl.logs[timestamp] = rollbackLog

	return rl.logs[timestamp], nil
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
