package logs

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/file"
)

var QueryLogManagerTickInterval = time.Second * 1
var QueryLogManagerFlushThreshold = time.Minute * 1

type LogManager struct {
	context        context.Context
	deletingLogs   bool
	errorLogs      map[string]*ErrorLog
	queryLogBuffer sync.Pool
	queryLogs      map[string]*QueryLog
	mutex          *sync.Mutex
}

func NewLogManager(context context.Context) *LogManager {
	return &LogManager{
		context:   context,
		errorLogs: make(map[string]*ErrorLog),
		queryLogBuffer: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		queryLogs: make(map[string]*QueryLog),
		mutex:     &sync.Mutex{},
	}
}

func (lm *LogManager) Close() error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	for _, log := range lm.queryLogs {
		err := log.Close()

		if err != nil {
			return err
		}
	}

	lm.queryLogs = make(map[string]*QueryLog)

	for _, log := range lm.errorLogs {
		err := log.Close()

		if err != nil {
			return err
		}
	}

	lm.errorLogs = make(map[string]*ErrorLog)

	return nil
}

func (lm *LogManager) GetQueryLog(cluster *cluster.Cluster, databaseHash, databaseId, branchId string) *QueryLog {
	// Get the current time un UTC
	t := time.Now().UTC()

	// Set the timestamp to the start of the day
	timestamp := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// If the date has changed, close the current log file and remove to reopen.
	if log, ok := lm.queryLogs[databaseHash]; ok && lm.queryLogs[databaseHash].timestamp != timestamp.UTC().Unix() {
		go func() {
			if err := log.Close(); err != nil {
				slog.Error("Error closing query log", "error", err)
			}
		}()

		delete(lm.queryLogs, databaseHash)
	}

	if _, ok := lm.queryLogs[databaseHash]; !ok {
		path := fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(databaseId, branchId))

		ctx, cancel := context.WithCancel(context.Background())

		lm.queryLogs[databaseHash] = &QueryLog{
			branchId:     branchId,
			cancel:       cancel,
			context:      ctx,
			cluster:      cluster,
			databaseHash: databaseHash,
			databaseId:   databaseId,
			encrypted:    false,
			keyBuffer:    bytes.NewBuffer(make([]byte, 20)),
			mutex:        sync.RWMutex{},
			path:         path,
			queryHasher:  crc64.New(crc64.MakeTable(crc64.ISO)),
			queue:        make(map[time.Time]map[uint64]*QueryMetric),
			tieredFS:     cluster.TmpTieredFS(),
			timestamp:    timestamp.UTC().Unix(),
		}
	}

	return lm.queryLogs[databaseHash]
}

func (lm *LogManager) GetErrorLog(cluster *cluster.Cluster, databaseHash, databaseId, branchId string) *ErrorLog {
	// Get the current time in UTC
	t := time.Now().UTC()

	// Set the timestamp to the start of the day
	timestamp := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// If the date has changed, close the current log file and remove to reopen.
	if log, ok := lm.errorLogs[databaseHash]; ok && lm.errorLogs[databaseHash].timestamp != timestamp.UTC().Unix() {
		go func() {
			if err := log.Close(); err != nil {
				slog.Error("Error closing error log", "error", err)
			}
		}()

		delete(lm.errorLogs, databaseHash)
	}

	if _, ok := lm.errorLogs[databaseHash]; !ok {
		path := fmt.Sprintf("%slogs/error", file.GetDatabaseFileBaseDir(databaseId, branchId))

		ctx, cancel := context.WithCancel(context.Background())

		lm.errorLogs[databaseHash] = &ErrorLog{
			branchId:     branchId,
			cancel:       cancel,
			context:      ctx,
			cluster:      cluster,
			databaseHash: databaseHash,
			databaseId:   databaseId,
			encrypted:    false,
			mutex:        sync.RWMutex{},
			path:         path,
			queue:        make([]*ErrorEntry, 0),
			tmpFS:        cluster.TmpFS(),
			timestamp:    timestamp.UTC().Unix(),
		}
	}

	return lm.errorLogs[databaseHash]
}

func (lm *LogManager) Error(entry ErrorLogEntry) error {
	l := lm.GetErrorLog(
		entry.Cluster,
		entry.DatabaseHash,
		entry.DatabaseID,
		entry.BranchID,
	)

	if l == nil {
		return nil
	}

	go func() {
		if err := l.Write(
			entry.CredentialID,
			entry.Statement,
			entry.Error,
			entry.Latency,
		); err != nil {
			slog.Error("Error writing error log", "error", err)
		}
	}()

	return nil
}

func (lm *LogManager) Query(entry QueryLogEntry) error {
	l := lm.GetQueryLog(
		entry.Cluster,
		entry.DatabaseHash,
		entry.DatabaseID,
		entry.BranchID,
	)

	if l == nil {
		return nil
	}

	go func() {
		if err := l.Write(
			entry.CredentialID,
			entry.Statement,
			entry.Latency,
		); err != nil {
			slog.Error("Error writing query log", "error", err)
		}
	}()

	return nil
}

func (lm *LogManager) Run() {
	ticker := time.NewTicker(QueryLogManagerTickInterval)

	for {
		select {
		case <-lm.context.Done():
			return
		case <-ticker.C:
			if lm.deletingLogs {
				continue
			}

			lm.mutex.Lock()
			lm.deletingLogs = true

			// Close query logs that have not been used in the last 5 minutes.
			for _, l := range lm.queryLogs {
				l.mutex.Lock()
				sinceLastLogged := time.Since(l.lastLoggedTime)
				l.mutex.Unlock()

				if sinceLastLogged > QueryLogManagerFlushThreshold {
					err := l.Close()

					if err != nil {
						slog.Error("Error closing query log", "error", err)
					}

					delete(lm.queryLogs, l.databaseHash)
				}
			}

			// Close error logs that have not been used in the last 5 minutes.
			for _, l := range lm.errorLogs {
				l.mutex.Lock()
				sinceLastLogged := time.Since(l.lastLoggedTime)
				l.mutex.Unlock()

				if sinceLastLogged > QueryLogManagerFlushThreshold {
					err := l.Close()

					if err != nil {
						slog.Error("Error closing error log", "error", err)
					}

					delete(lm.errorLogs, l.databaseHash)
				}
			}

			lm.deletingLogs = false
			lm.mutex.Unlock()
		}
	}
}
