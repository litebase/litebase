package storage

import (
	"fmt"
	"iter"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

type TieredFileSystemLogger struct {
	currentLog int64
	directory  string
	entries    map[int64]map[string]struct{}
	files      map[int64]internalStorage.File
	fileSystem FileSystemDriver
	mutex      *sync.Mutex
}

type TieredFileSystemLoggerEntry struct {
	Key string
	Log int64
}

func NewTieredFileSystemLogger(fileSystem FileSystemDriver, directory string) (*TieredFileSystemLogger, error) {
	return &TieredFileSystemLogger{
		currentLog: time.Now().UTC().UnixNano(),
		directory:  directory,
		entries:    make(map[int64]map[string]struct{}),
		files:      make(map[int64]internalStorage.File),
		fileSystem: fileSystem,
		mutex:      &sync.Mutex{},
	}, nil
}

func (tfl *TieredFileSystemLogger) Close() error {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	for _, file := range tfl.files {
		if err := file.Close(); err != nil {
			return err
		}
	}

	tfl.files = make(map[int64]internalStorage.File)

	return nil
}

func (tfl *TieredFileSystemLogger) file() (internalStorage.File, error) {
	if file, ok := tfl.files[tfl.currentLog]; ok {
		return file, nil
	}

tryOpen:
	file, err := tfl.fileSystem.OpenFile(
		fmt.Sprintf("%s/%d", tfl.directory, tfl.currentLog),
		os.O_CREATE|os.O_WRONLY,
		0600,
	)

	if err != nil {
		if os.IsNotExist(err) {
			if err := tfl.fileSystem.MkdirAll(tfl.directory, 0750); err != nil {
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	tfl.files[tfl.currentLog] = file

	return file, err
}

func (tfl *TieredFileSystemLogger) DirtyKeys() iter.Seq[TieredFileSystemLoggerEntry] {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	return func(yield func(TieredFileSystemLoggerEntry) bool) {
		// Check if the directory exists
		if _, err := tfl.fileSystem.Stat(tfl.directory); os.IsNotExist(err) {
			return
		}

		// Check if there are any log files in the directory
		files, err := tfl.fileSystem.ReadDir(tfl.directory)

		if err != nil {
			return
		}

		// Iterate over the files and collect the keys that are present
	outer:
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			logFile, err := tfl.fileSystem.Open(tfl.directory + "/" + file.Name())

			if err != nil {
				continue
			}

			logInt64Key, err := strconv.ParseInt(file.Name(), 10, 64)
			if err != nil {
				slog.Error("Error parsing log file name to int64:", "error", err)
				_ = logFile.Close()
				continue
			}

			for {
				entry := TieredFileSystemLoggerEntry{
					Log: logInt64Key,
				}

				_, err := fmt.Fscanln(logFile, &entry.Key)

				if err != nil {
					break
				}

				if !yield(entry) {
					err = logFile.Close()

					if err != nil {
						slog.Error("Error closing log file:", "error", err)
					}

					break outer
				}
			}

			err = logFile.Close()

			if err != nil {
				slog.Error("Error closing log file:", "error", err)
			}
		}
	}
}

func (tfl *TieredFileSystemLogger) File() (internalStorage.File, error) {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	return tfl.file()
}

// Check the logs directory to see if there are any logs that contain file keys
// that need to be flushed to durable storage.
func (tfl *TieredFileSystemLogger) HasDirtyLogs() bool {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	// Check if the directory exists
	if _, err := tfl.fileSystem.Stat(tfl.directory); os.IsNotExist(err) {
		return false
	}

	// Check if there are any log files in the directory
	files, err := tfl.fileSystem.ReadDir(tfl.directory)

	if err != nil {
		return false
	}

	dirtyLogCount := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		dirtyLogCount++
	}

	return dirtyLogCount > 0
}

func (tfl *TieredFileSystemLogger) Put(key string) (int64, error) {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	if _, ok := tfl.entries[tfl.currentLog]; !ok {
		tfl.entries[tfl.currentLog] = make(map[string]struct{})
	}

	if _, ok := tfl.entries[tfl.currentLog][key]; ok {
		return tfl.currentLog, nil
	}

	tfl.entries[tfl.currentLog][key] = struct{}{}

	file, err := tfl.file()

	if err != nil {
		return 0, err
	}

	tfl.files[tfl.currentLog] = file

	if _, err := fmt.Fprintf(file, "%s\n", key); err != nil {
		return 0, err
	}

	return tfl.currentLog, nil
}

// Remove a key from the log. If the log is empty after removing the key,
// the log file is deleted.
func (tfl *TieredFileSystemLogger) Remove(key string, logKey int64) error {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	if _, ok := tfl.entries[logKey]; !ok {
		return nil
	}

	if _, ok := tfl.entries[logKey][key]; !ok {
		return nil
	}

	delete(tfl.entries[logKey], key)

	if len(tfl.entries[logKey]) == 0 {
		delete(tfl.entries, logKey)

		err := tfl.removeLogFile(logKey)

		if err != nil {
			return err
		}
	}

	return nil
}

func (tfl *TieredFileSystemLogger) Restart() error {
	tfl.mutex.Lock()
	defer tfl.mutex.Unlock()

	tfl.entries = make(map[int64]map[string]struct{})
	tfl.setCurrentLog()

	// Remove the old log files
	files, err := tfl.fileSystem.ReadDir(tfl.directory)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		err := tfl.fileSystem.Remove(fmt.Sprintf("%s/%s", tfl.directory, file.Name()))

		if err != nil {
			log.Println("Error removing log file", file.Name(), ":", err)
		}
	}

	return nil
}

// Remove the log file for the given log key
func (tfl *TieredFileSystemLogger) removeLogFile(log int64) error {
	if file, ok := tfl.files[log]; ok {
		if err := file.Close(); err != nil {
			return err
		}

		delete(tfl.files, log)

		if err := tfl.fileSystem.Remove(fmt.Sprintf("%s/%d", tfl.directory, log)); err != nil {
			return err
		}

		tfl.setCurrentLog()
	}

	return nil
}

// Set the current log to the current time in nanoseconds.
func (tfl *TieredFileSystemLogger) setCurrentLog() {
	tfl.currentLog = time.Now().UTC().UnixNano()
}
