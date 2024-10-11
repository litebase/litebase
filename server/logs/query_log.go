package logs

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	internalStorage "litebase/internal/storage"
	"litebase/server/cluster"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type QueryLog struct {
	branchId       string
	databaseId     string
	file           internalStorage.File
	keyBuffer      *bytes.Buffer
	mutex          sync.RWMutex
	path           string
	queryHasher    hash.Hash64
	queue          map[time.Time]map[uint64]*QueryMetric
	statementIndex *QueryIndex
	timestamp      int64
	watching       bool
}

type QueryLogEnry struct {
	DatabaseHash, DatabaseId, BranchId, AccessKeyId, Statement string
	Latency                                                    float64
}

var queryLoggers = make(map[string]*QueryLog)
var qyeryLogMutex = sync.Mutex{}
var queryLogBuffer = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

func GetQueryLog(databaseHash, databaseId, branchId string) *QueryLog {
	qyeryLogMutex.Lock()
	defer qyeryLogMutex.Unlock()

	// Get the current time un UTC
	t := time.Now().UTC()

	// Set the timestamp to the start of the day
	timestamp := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	if _, ok := queryLoggers[databaseHash]; !ok {
		path := fmt.Sprintf("%s/logs/query", file.GetDatabaseFileBaseDir(databaseId, branchId))

		queryLoggers[databaseHash] = &QueryLog{
			branchId:    branchId,
			databaseId:  databaseId,
			keyBuffer:   bytes.NewBuffer(make([]byte, 20)),
			mutex:       sync.RWMutex{},
			path:        path,
			queryHasher: crc64.New(crc64.MakeTable(crc64.ISO)),
			queue:       make(map[time.Time]map[uint64]*QueryMetric),
			timestamp:   timestamp.UTC().Unix(),
		}
	}

	// If the date has changed, close the current log file and set the timestamp
	// to the current date.
	if queryLoggers[databaseHash].timestamp != timestamp.UTC().Unix() {
		queryLoggers[databaseHash].timestamp = timestamp.UTC().Unix()

		queryLoggers[databaseHash].file.Close()
		queryLoggers[databaseHash].file = nil

		queryLoggers[databaseHash].statementIndex.Close()
		queryLoggers[databaseHash].statementIndex = nil
	}

	return queryLoggers[databaseHash]
}

func Query(entry QueryLogEnry) error {
	log := GetQueryLog(
		entry.DatabaseHash,
		entry.DatabaseId,
		entry.BranchId,
	)

	if log == nil {
		return nil
	}

	log.Write(
		entry.AccessKeyId,
		entry.Statement,
		entry.Latency,
	)

	return nil
}

func (q *QueryLog) GetFile() internalStorage.File {
	if q.file == nil {
		path := fmt.Sprintf("%s/%d/QUERY_LOG_%s", q.path, q.timestamp, cluster.Node().Id)

		err := storage.TieredFS().MkdirAll(filepath.Dir(path), 0755)

		if err != nil {
			log.Fatal(err)
		}

		file, err := storage.TieredFS().OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

		if err != nil {
			log.Fatal(err)
		}

		q.file = file
	}

	return q.file
}

func (q *QueryLog) GetStatementIndex() *QueryIndex {
	if q.statementIndex == nil {
		statementIndex, err := GetQueryIndex(q.path, "STATEMENT_IDX", q.timestamp)

		if err != nil {
			if os.IsNotExist(err) {
				storage.TieredFS().MkdirAll(q.path, 0755)
			} else {
				log.Fatal(err)
			}
		}

		q.statementIndex = statementIndex
	}

	return q.statementIndex
}

func (q *QueryLog) Flush() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	file := q.GetFile()

	data := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(data)

	for timestamp, metrics := range q.queue {
		if !timestamp.Before(time.Now().UTC().Truncate(time.Second)) {
			continue
		}

		for checksum, metric := range metrics {
			data.Reset()

			_, err := file.Write(metric.Bytes(data))

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

func (q *QueryLog) Read(start, end uint32) []QueryMetric {
	queryMetrics := make([]QueryMetric, 0)
	timeInstance := time.Unix(int64(start), 0)
	startOfDay := uint32(time.Date(timeInstance.Year(), timeInstance.Month(), timeInstance.Day(), 0, 0, 0, 0, time.UTC).Unix())

	// Get all the directories in the logs directory
	path := fmt.Sprintf(
		"%s/logs/query",
		file.GetDatabaseFileBaseDir(q.databaseId, q.branchId),
	)

	dirs, err := storage.TieredFS().ReadDir(path)

	if err != nil {
		if os.IsNotExist(err) {
			return queryMetrics
		}

		log.Fatal(err)
	}

	for directory := range dirs {
		if !dirs[directory].IsDir {
			continue
		}

		// Get the timestamp of the directory
		directoryTimestamp, err := strconv.ParseInt(dirs[directory].Name, 10, 64)

		if err != nil {
			log.Println(err)
			break
		}

		// If the timestamp is not in the range, skip it
		if uint32(directoryTimestamp) < startOfDay || uint32(directoryTimestamp) > end {
			continue
		}

		// Read all the files in the directory
		files, err := storage.TieredFS().ReadDir(fmt.Sprintf("%s/%d/", path, directoryTimestamp))

		if err != nil {
			if os.IsNotExist(err) {
				return queryMetrics
			}

			log.Fatal(err)
		}

		for _, entry := range files {
			if entry.IsDir {
				continue
			}

			file, err := storage.TieredFS().Open(fmt.Sprintf("%s/%d/%s", path, directoryTimestamp, entry.Name))

			if err != nil {
				log.Println(err)
				break
			}

			defer file.Close()

			// read 64 bytes at a time
			fileBuffer := make([]byte, 64)

			for {
				_, err := file.Read(fileBuffer)

				if err != nil {
					break
				}

				queryMetric := QueryMetricFromBytes(fileBuffer)

				// if the timstamp is not in the range continue
				if queryMetric.Timestamp < start || queryMetric.Timestamp > end {
					continue
				}

				// Add the data to the array
				queryMetrics = append(queryMetrics, queryMetric)
			}
		}
	}

	return queryMetrics
}

func (q *QueryLog) Watch() {
	if q.watching {
		return
	}

	q.watching = true

	go func() {
		ticker := time.NewTicker(1 * time.Second)

		for {
			select {
			case <-cluster.Node().Context().Done():
				return
			case <-ticker.C:
				q.mutex.RLock()

				if len(q.queue) == 0 {
					q.mutex.RUnlock()
					q.watching = false
					ticker.Stop()
					return
				}

				q.mutex.RUnlock()

				q.Flush()
			}
		}
	}()
}

func (q *QueryLog) Write(accessKeyId, statement string, latency float64) {

	timestamp := time.Now().UTC().Truncate(time.Second)

	buffer := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(buffer)
	buffer.Reset()

	buffer.WriteString("access_key_id=")
	buffer.WriteString(accessKeyId)
	buffer.WriteString(" statement=")

	// Lowercase the statement
	statementBytes := []byte(statement)

	for i := 0; i < len(statement); i++ {
		char := statementBytes[i]
		if char >= 'A' && char <= 'Z' {
			char += 'a' - 'A'
		}

		buffer.WriteByte(char)
	}

	logData := buffer.Bytes()
	q.queryHasher.Reset()
	q.queryHasher.Write(logData)
	checksum := q.queryHasher.Sum64()
	// Convert the checksum to a hexadecimal
	q.keyBuffer.Reset()
	// key := strconv.FormatUint(checksum, 16)

	q.keyBuffer.Write(strconv.AppendUint(q.keyBuffer.Bytes()[:0], checksum, 16))

	// Check if the statement is already in the dictionary
	if _, ok := q.GetStatementIndex().Get(q.keyBuffer.String()); !ok {
		err := q.GetStatementIndex().Set(q.keyBuffer.String(), string(logData))

		if err != nil {
			log.Fatal(err)
		}
	}

	if !q.watching {
		q.mutex.Lock()
		shouldWatch := !q.watching

		if shouldWatch {
			q.Watch()
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

		return
	}

	metric.AddLatency(latency)
}
