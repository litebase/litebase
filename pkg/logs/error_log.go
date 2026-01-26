package logs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/storage"
)

var ErrorLogFlushInterval = time.Second * 5

type ErrorLog struct {
	branchId       string
	cancel         context.CancelFunc
	cluster        *cluster.Cluster
	context        context.Context
	databaseHash   string
	databaseId     string
	dataKey        []byte // Optional: 32-byte encryption key
	encrypted      bool   // Whether error logs should be encrypted
	file           internalStorage.File
	keyHash        [32]byte // Optional: SHA256 hash of encryption key
	lastLoggedTime time.Time
	mutex          sync.RWMutex
	path           string
	queue          []*ErrorEntry
	timestamp      int64
	tmpFS          *storage.FileSystem
	watching       bool
}

type ErrorLogEntry struct {
	Cluster                                          *cluster.Cluster
	DatabaseHash, DatabaseID, BranchID, CredentialID string
	Statement                                        string
	Error                                            string
	Latency                                          float64
}

type ErrorEntry struct {
	Timestamp    uint32
	CredentialID string
	Statement    string
	Error        string
	Latency      float64
}

// ConfigureEncryption sets the encryption parameters for the ErrorLog.
// This must be called before any error logs are written.
func (e *ErrorLog) ConfigureEncryption(dataKey []byte, keyHash [32]byte) error {
	if len(dataKey) != 32 {
		return fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.dataKey = dataKey
	e.keyHash = keyHash
	e.encrypted = true

	return nil
}

// IsEncrypted returns whether encryption is configured for this ErrorLog.
func (e *ErrorLog) IsEncrypted() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.encrypted
}

func (e *ErrorLog) Close() error {
	// Flush before closing
	err := e.Flush(true)

	if err != nil {
		slog.Error("Error flushing error log before close", "error", err)
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.cancel()

	if e.file != nil {
		err := e.file.Close()

		if err != nil {
			return err
		}

		e.file = nil
	}

	return nil
}

// Flush writes queued error entries to storage
func (e *ErrorLog) Flush(force bool) error {
	e.mutex.Lock()

	if !force && len(e.queue) == 0 {
		e.mutex.Unlock()
		return nil
	}

	// Make a copy of the queue to minimize lock time
	queueCopy := make([]*ErrorEntry, len(e.queue))
	copy(queueCopy, e.queue)
	e.queue = e.queue[:0] // Clear queue

	e.mutex.Unlock()

	if len(queueCopy) == 0 {
		return nil
	}

	// Write entries to file
	for _, entry := range queueCopy {
		err := e.writeEntry(entry)

		if err != nil {
			slog.Error("Failed to write error entry", "error", err)
			return err
		}
	}

	return nil
}

// GetFile returns the error log file, creating it if necessary
func (e *ErrorLog) GetFile() (internalStorage.File, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.file != nil {
		return e.file, nil
	}

	logPath := fmt.Sprintf("%s-%d.log", e.path, e.timestamp)

tryOpen:
	// Use encrypted stream file if encryption is configured
	if e.encrypted {
		f, err := e.tmpFS.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err := e.tmpFS.MkdirAll(filepath.Dir(logPath), 0750)

				if err != nil {
					return nil, err
				}

				goto tryOpen
			}

			return nil, err
		}

		encryptedFile, err := storage.NewEncryptedStreamFile(
			f,
			e.dataKey,
			e.keyHash,
			0,       // File offset (0 for new files)
			logPath, // Path for error messages
		)

		if err != nil {
			return nil, err
		}

		e.file = encryptedFile

		return e.file, nil
	}

	// Open regular file
	f, err := e.tmpFS.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err := e.tmpFS.MkdirAll(filepath.Dir(logPath), 0750)

			if err != nil {
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	e.file = f

	return e.file, nil
}

// Read reads error logs from storage for a time range
func (e *ErrorLog) Read(startTimestamp, endTimestamp uint32) ([]*ErrorEntry, error) {
	// Get the log file path
	logPath := fmt.Sprintf("%s-%d.log", e.path, e.timestamp)

	// Check if file exists
	if _, err := e.tmpFS.Stat(logPath); os.IsNotExist(err) {
		return []*ErrorEntry{}, nil
	}

	var f internalStorage.File
	var err error

	// Open file with encryption if configured
	if e.encrypted {
		baseFile, err := e.tmpFS.Open(logPath)

		if err != nil {
			return []*ErrorEntry{}, nil
		}

		f, err = storage.OpenEncryptedStreamFile(
			baseFile,
			e.dataKey,
			e.keyHash,
			0,       // File offset
			logPath, // Path for error messages
		)

		if err != nil {
			return nil, err
		}
	} else {
		f, err = e.tmpFS.Open(logPath)

		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if err := f.Close(); err != nil {
			slog.Error("Error closing error log file", "error", err)
		}
	}()

	var entries []*ErrorEntry

	for {
		// Read entry from file
		entry, err := e.readEntry(f, startTimestamp, endTimestamp)

		if err != nil {
			if err.Error() == "EOF" {
				break
			}

			return nil, err
		}

		if entry != nil {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// readEntry reads a single error entry from the file
func (e *ErrorLog) readEntry(f internalStorage.File, startTimestamp, endTimestamp uint32) (*ErrorEntry, error) {
	// Read timestamp (4 bytes)
	timestampBytes := make([]byte, 4)

	n, err := f.Read(timestampBytes)

	if err != nil || n == 0 {
		return nil, fmt.Errorf("EOF")
	}

	timestamp := binary.LittleEndian.Uint32(timestampBytes)

	// Skip if outside range
	if timestamp < startTimestamp || timestamp > endTimestamp {
		// Still need to skip the rest of the entry
		return nil, e.skipEntry(f)
	}

	// Read credential ID length (2 bytes)
	credLenBytes := make([]byte, 2)

	_, err = f.Read(credLenBytes)

	if err != nil {
		return nil, err
	}

	credLen := binary.LittleEndian.Uint16(credLenBytes)

	// Read credential ID
	credIDBytes := make([]byte, credLen)

	_, err = f.Read(credIDBytes)

	if err != nil {
		return nil, err
	}

	// Read statement length (4 bytes)
	stmtLenBytes := make([]byte, 4)

	_, err = f.Read(stmtLenBytes)

	if err != nil {
		return nil, err
	}

	stmtLen := binary.LittleEndian.Uint32(stmtLenBytes)

	// Read statement - create new slice to avoid reusing buffer
	stmtBytes := make([]byte, stmtLen)

	_, err = f.Read(stmtBytes)

	if err != nil {
		return nil, err
	}

	// Read error length (4 bytes)
	errLenBytes := make([]byte, 4)

	_, err = f.Read(errLenBytes)

	if err != nil {
		return nil, err
	}

	errLen := binary.LittleEndian.Uint32(errLenBytes)

	// Read error message - create new slice to avoid reusing buffer
	errBytes := make([]byte, errLen)

	_, err = f.Read(errBytes)

	if err != nil {
		return nil, err
	}

	// Read latency (8 bytes float64)
	latencyBytes := make([]byte, 8)

	_, err = f.Read(latencyBytes)

	if err != nil {
		return nil, err
	}

	latency := math.Float64frombits(binary.LittleEndian.Uint64(latencyBytes))

	return &ErrorEntry{
		Timestamp:    timestamp,
		CredentialID: string(credIDBytes),
		Statement:    string(stmtBytes),
		Error:        string(errBytes),
		Latency:      latency,
	}, nil
}

// skipEntry skips the rest of an error entry
func (e *ErrorLog) skipEntry(f internalStorage.File) error {
	// Read credential ID length and skip
	credLenBytes := make([]byte, 2)

	_, err := f.Read(credLenBytes)

	if err != nil {
		return err
	}

	credLen := binary.LittleEndian.Uint16(credLenBytes)

	if _, err := f.Seek(int64(credLen), 1); err != nil {
		return err
	}

	// Read statement length and skip
	stmtLenBytes := make([]byte, 4)

	_, err = f.Read(stmtLenBytes)

	if err != nil {
		return err
	}

	stmtLen := binary.LittleEndian.Uint32(stmtLenBytes)

	if _, err := f.Seek(int64(stmtLen), 1); err != nil {
		return err
	}

	// Read error length and skip
	errLenBytes := make([]byte, 4)

	_, err = f.Read(errLenBytes)

	if err != nil {
		return err
	}

	errLen := binary.LittleEndian.Uint32(errLenBytes)

	if _, err := f.Seek(int64(errLen), 1); err != nil {
		return err
	}

	// Skip latency (8 bytes)
	if _, err := f.Seek(8, 1); err != nil {
		return err
	}

	return nil
}

// Watch starts background flushing of queued entries
func (e *ErrorLog) Watch() {
	if e.watching {
		return
	}

	e.watching = true

	go func() {
		ticker := time.NewTicker(ErrorLogFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-e.context.Done():
				return
			case <-ticker.C:
				if err := e.Flush(false); err != nil {
					slog.Error("Failed to flush error log", "error", err)
				}
			}
		}
	}()
}

// Write queues an error entry for writing
func (e *ErrorLog) Write(credentialID, statement, errorMsg string, latency float64) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Start watching if not already
	if !e.watching {
		e.mutex.Unlock()
		e.Watch()
		e.mutex.Lock()
	}

	timestamp, err := utils.SafeInt64ToUint32(time.Now().UTC().Unix())

	if err != nil {
		return err
	}

	entry := &ErrorEntry{
		Timestamp:    timestamp,
		CredentialID: credentialID,
		Statement:    statement,
		Error:        errorMsg,
		Latency:      latency,
	}

	e.queue = append(e.queue, entry)
	e.lastLoggedTime = time.Now().UTC()

	return nil
}

// writeEntry writes a single error entry to the file
func (e *ErrorLog) writeEntry(entry *ErrorEntry) error {
	f, err := e.GetFile()

	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 1024))

	// Write timestamp (4 bytes)
	timestampBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timestampBytes, entry.Timestamp)
	buffer.Write(timestampBytes)

	// Write credential ID length (2 bytes) and value
	credIDBytes := []byte(entry.CredentialID)
	credLenBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(credLenBytes, uint16(len(credIDBytes)))
	buffer.Write(credLenBytes)
	buffer.Write(credIDBytes)

	// Write statement length (4 bytes) and value
	stmtBytes := []byte(entry.Statement)
	stmtLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(stmtLenBytes, uint32(len(stmtBytes)))
	buffer.Write(stmtLenBytes)
	buffer.Write(stmtBytes)

	// Write error length (4 bytes) and value
	errBytes := []byte(entry.Error)
	errLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(errLenBytes, uint32(len(errBytes)))
	buffer.Write(errLenBytes)
	buffer.Write(errBytes)

	// Write latency (8 bytes float64)
	latencyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(latencyBytes, math.Float64bits(entry.Latency))
	buffer.Write(latencyBytes)

	// Write to file
	_, err = f.Write(buffer.Bytes())

	return err
}

// Bytes returns the byte representation of an ErrorEntry for storage
func (e *ErrorEntry) Bytes() ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))

	// Write timestamp
	timestampBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timestampBytes, e.Timestamp)
	buffer.Write(timestampBytes)

	// Write credential ID
	credIDBytes := []byte(e.CredentialID)
	credLenBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(credLenBytes, uint16(len(credIDBytes)))
	buffer.Write(credLenBytes)
	buffer.Write(credIDBytes)

	// Write statement
	stmtBytes := []byte(e.Statement)
	stmtLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(stmtLenBytes, uint32(len(stmtBytes)))
	buffer.Write(stmtLenBytes)
	buffer.Write(stmtBytes)

	// Write error
	errBytes := []byte(e.Error)
	errLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(errLenBytes, uint32(len(errBytes)))
	buffer.Write(errLenBytes)
	buffer.Write(errBytes)

	// Write latency
	latencyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(latencyBytes, math.Float64bits(e.Latency))
	buffer.Write(latencyBytes)

	return buffer.Bytes(), nil
}
