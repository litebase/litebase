package logs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

var QueryLogFlushInterval = time.Second * 5
var QueryLogFlushThreshold = time.Second

var queryLogBuffer = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

type QueryLog struct {
	cancel         context.CancelFunc
	branchId       string
	context        context.Context
	cluster        *cluster.Cluster
	databaseHash   string
	databaseId     string
	file           internalStorage.File
	keyBuffer      *bytes.Buffer
	lastLoggedTime time.Time
	mutex          sync.RWMutex
	path           string
	queryHasher    hash.Hash64
	queue          map[time.Time]map[uint64]*QueryMetric
	statementIndex *QueryStatementIndex
	tieredFS       *storage.FileSystem
	timestamp      int64
	watching       bool
}

type QueryLogEntry struct {
	Cluster                                         *cluster.Cluster
	DatabaseHash, DatabaseID, BranchID, AccessKeyID string
	Statement                                       string
	Latency                                         float64
}

func (q *QueryLog) Close() error {
	// Flush before closing to avoid deadlock
	q.Flush(true)

	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.cancel()

	if q.file != nil {
		err := q.file.Close()

		if err != nil {
			return err
		}

		q.file = nil
	}

	if q.statementIndex != nil {
		err := q.statementIndex.Close()

		if err != nil {
			return err
		}

		q.statementIndex = nil
	}

	return nil
}

func (q *QueryLog) GetFile() internalStorage.File {
	if q.file == nil {
		path := fmt.Sprintf("%s/%d/QUERY_LOG_%s", q.path, q.timestamp, q.cluster.Node().ID)

	tryOpen:
		file, err := q.tieredFS.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err := q.tieredFS.MkdirAll(filepath.Dir(path), 0750)

				if err != nil {
					log.Println(err)
				}

				goto tryOpen
			}

			log.Println(err)
		}

		q.file = file
	}

	return q.file
}

func (q *QueryLog) GetStatementIndex() (*QueryStatementIndex, error) {
	if q.statementIndex == nil {
		statementIndex, err := GetQueryStatementIndex(
			q.tieredFS,
			q.path,
			fmt.Sprintf("QUERY_STATEMENT_INDEX_%s", q.cluster.Node().ID),
			q.timestamp,
		)

		if err != nil {
			if os.IsNotExist(err) {
				err := q.tieredFS.MkdirAll(q.path, 0750)

				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		q.statementIndex = statementIndex
	}

	return q.statementIndex, nil
}

func (q *QueryLog) Flush(force bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	file := q.GetFile()

	data := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(data)

	for timestamp, metrics := range q.queue {
		if !force && !timestamp.Before(time.Now().UTC().Truncate(QueryLogFlushThreshold)) {
			continue
		}

		for checksum, metric := range metrics {
			data.Reset()

			metricBytes, err := metric.Bytes(data)

			if err != nil {
				slog.Error("Error converting query metric to bytes", "error", err)
				continue
			}

			_, err = file.Write(metricBytes)

			if err != nil {
				if errors.Is(err, os.ErrClosed) {
					log.Fatal(err)
				}

				log.Println(err)
			}

			delete(metrics, checksum)
		}

		delete(q.queue, timestamp)
	}

	err := file.Sync()

	if err != nil {
		log.Println(err)
	}
}

func (q *QueryLog) Read(start, end uint32) ([]QueryMetric, error) {
	queryMetrics := make([]QueryMetric, 0)
	// uint64Start
	timeInstance := time.Unix(int64(start), 0).UTC()
	startOfDay, err := utils.SafeInt64ToUint32(time.Date(timeInstance.Year(), timeInstance.Month(), timeInstance.Day(), 0, 0, 0, 0, time.UTC).Unix())

	if err != nil {
		return nil, err
	}

	// Get all the directories in the logs directory
	path := fmt.Sprintf(
		"%slogs/query",
		file.GetDatabaseFileBaseDir(q.databaseId, q.branchId),
	)

	dirs, err := q.tieredFS.ReadDir(path)

	if err != nil {
		if os.IsNotExist(err) {
			return queryMetrics, nil
		}

		return nil, err
	}

	for directory := range dirs {
		if !dirs[directory].IsDir() {
			continue
		}

		// Get the timestamp of the directory
		directoryTimestamp, err := strconv.ParseInt(dirs[directory].Name(), 10, 64)

		if err != nil {
			return nil, err
		}

		uint32DirectoryTimestamp, err := utils.SafeInt64ToUint32(directoryTimestamp)

		if err != nil {
			return nil, err
		}

		// If the timestamp is not in the range, skip it
		if uint32DirectoryTimestamp < startOfDay || uint32DirectoryTimestamp > end {
			continue
		}

		// Read all the files in the directory
		files, err := q.tieredFS.ReadDir(fmt.Sprintf("%s/%d/", path, directoryTimestamp))

		if err != nil {
			if os.IsNotExist(err) {
				return queryMetrics, nil
			}

			return nil, err
		}

		for _, entry := range files {
			if entry.IsDir() {
				continue
			}

			file, err := q.tieredFS.Open(fmt.Sprintf("%s/%d/%s", path, directoryTimestamp, entry.Name()))

			if err != nil {
				return nil, err
			}

			defer file.Close()

			// read 64 bytes at a time
			fileBuffer := make([]byte, 64)

			for {
				_, err := file.Read(fileBuffer)

				if err != nil {
					break
				}

				queryMetric, err := QueryMetricFromBytes(fileBuffer)

				if err != nil {
					slog.Error("Error reading query metric", "error", err)
					continue
				}

				// if the timestamp is not in the range continue
				if queryMetric.Timestamp < start || queryMetric.Timestamp > end {
					continue
				}

				// Add the data to the array
				queryMetrics = append(queryMetrics, queryMetric)
			}
		}
	}

	return queryMetrics, nil
}

func (q *QueryLog) watch() {
	if q.watching {
		return
	}

	q.watching = true

	go func() {
		ticker := time.NewTicker(QueryLogFlushInterval)

		for {
			select {
			case <-q.cluster.Node().Context().Done():
				return
			case <-q.context.Done():
				return
			case <-ticker.C:
				q.mutex.RLock()
				queueLen := len(q.queue)
				q.mutex.RUnlock()

				if queueLen == 0 {
					q.mutex.Lock()

					if len(q.queue) == 0 {
						q.watching = false
						q.mutex.Unlock()
						ticker.Stop()
						return
					}

					q.mutex.Unlock()
				} else {
					q.Flush(false)
				}
			}
		}
	}()
}

func (q *QueryLog) Write(accessKeyId string, statement string, latency float64) error {
	q.lastLoggedTime = time.Now().UTC()
	timestamp := time.Now().UTC().Truncate(time.Second)

	buffer := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(buffer)
	buffer.Reset()

	buffer.WriteString("access_key_id=")
	buffer.WriteString(accessKeyId)
	buffer.WriteString(" statement=")

	// Lowercase the statement
	statementBytes := []byte(statement)

	for i := range statement {
		char := statementBytes[i]
		if char >= 'A' && char <= 'Z' {
			char += 'a' - 'A'
		}

		buffer.WriteByte(char)
	}

	logData := buffer.Bytes()
	q.queryHasher.Reset()

	_, err := q.queryHasher.Write(logData)

	if err != nil {
		slog.Error("error writing to query hasher", "error", err)
		return err
	}

	checksum := q.queryHasher.Sum64()
	// Convert the checksum to a hexadecimal
	q.keyBuffer.Reset()

	q.keyBuffer.Write(strconv.AppendUint(q.keyBuffer.Bytes()[:0], checksum, 16))
	statementIndex, err := q.GetStatementIndex()

	if err != nil {
		slog.Error("error getting statement index", "error", err)
		return err
	}

	// Check if the statement is already in the dictionary
	if _, ok := statementIndex.Get(q.keyBuffer.String()); !ok {
		err := statementIndex.Set(q.keyBuffer.String(), string(logData))

		if err != nil {
			log.Println(err)
			return err
		}
	}

	if !q.watching {
		q.mutex.Lock()
		shouldWatch := !q.watching

		if shouldWatch {
			q.watch()
		}

		q.mutex.Unlock()
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if _, ok := q.queue[timestamp]; !ok {
		q.queue[timestamp] = map[uint64]*QueryMetric{}
	}

	metric, ok := q.queue[timestamp][checksum]

	if !ok {
		q.queue[timestamp][checksum] = NewQueryMetric(timestamp.UTC().Unix(), checksum)
		q.queue[timestamp][checksum].AddLatency(latency)

		return nil
	}

	metric.AddLatency(latency)

	return nil
}
