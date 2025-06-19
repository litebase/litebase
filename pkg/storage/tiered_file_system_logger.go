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
)

type TieredFileSystemLogger struct {
	currentLog int64
	directory  string
	entries    map[int64]map[string]struct{}
	files      map[int64]*os.File
	mutex      *sync.Mutex
}

type TieredFileSystemLoggerEntry struct {
	Key string
	Log int64
}

func NewTieredFileSystemLogger(directory string) (*TieredFileSystemLogger, error) {
	return &TieredFileSystemLogger{
		currentLog: time.Now().UTC().Unix(),
		directory:  directory,
		entries:    make(map[int64]map[string]struct{}),
		files:      make(map[int64]*os.File),
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

	tfl.files = make(map[int64]*os.File)

	return nil
}

func (tfl *TieredFileSystemLogger) file() (*os.File, error) {
	if file, ok := tfl.files[tfl.currentLog]; ok {
		return file, nil
	}

tryOpen:
	file, err := os.OpenFile(
		fmt.Sprintf("%s/%d", tfl.directory, tfl.currentLog),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0600,
	)

	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(tfl.directory, 0750); err != nil {
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
		if _, err := os.Stat(tfl.directory); os.IsNotExist(err) {
			return
		}

		// Check if there are any log files in the directory
		files, err := os.ReadDir(tfl.directory)

		if err != nil {
			return
		}

		// Iterate over the files and collect the keys that are present
	outer:
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			logFile, err := os.Open(tfl.directory + "/" + file.Name())

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

func (tfl *TieredFileSystemLogger) File() (*os.File, error) {
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
	if _, err := os.Stat(tfl.directory); os.IsNotExist(err) {
		return false
	}

	// Check if there are any log files in the directory
	files, err := os.ReadDir(tfl.directory)

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

	if _, err := file.WriteString(fmt.Sprintf("%s\n", key)); err != nil {
		return 0, err
	}

	return tfl.currentLog, nil
}

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
	tfl.currentLog = time.Now().UTC().Unix()

	// Remove the old log files
	files, err := os.ReadDir(tfl.directory)

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

		err := os.Remove(fmt.Sprintf("%s/%s", tfl.directory, file.Name()))

		if err != nil {
			log.Println("Error removing log file", file.Name(), ":", err)
		}
	}

	return nil
}

func (tfl *TieredFileSystemLogger) removeLogFile(log int64) error {
	if file, ok := tfl.files[log]; ok {
		if err := file.Close(); err != nil {
			return err
		}

		delete(tfl.files, log)
	}

	if err := os.Remove(fmt.Sprintf("%s/%d", tfl.directory, log)); err != nil {
		return err
	}

	return nil
}
