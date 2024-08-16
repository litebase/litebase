package logs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/node"
	"litebase/server/storage"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const QueryLogBufferSize = 100000

type QueryLog struct {
	branchUuid     string
	databaseUuid   string
	file           internalStorage.File
	mutex          sync.Mutex
	path           string
	queryHasher    hash.Hash64
	statementIndex *QueryIndex
	timestamp      int64
	watching       bool
	writeBuffer    chan []byte
}

type QueryLogEnry struct {
	DatabaseHash, DatabaseUuid, BranchUuid, AccessKeyId, Statement string
	Latency                                                        float64
}

var queryLoggers = make(map[string]*QueryLog)
var qyeryLogMutex = sync.Mutex{}
var queryLogBuffer = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

func GetQueryLog(databaseHash, databaseUuid, branchUuid string) *QueryLog {
	qyeryLogMutex.Lock()
	defer qyeryLogMutex.Unlock()

	// Get the current time un UTC
	t := time.Now().UTC()

	// Set the timestamp to the start of the day
	timestamp := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	if _, ok := queryLoggers[databaseHash]; !ok {
		path := fmt.Sprintf("%s/logs/query", file.GetDatabaseFileBaseDir(databaseUuid, branchUuid))

		statementIndex, err := GetQueryIndex(path, "STATEMENT_IDX")

		if err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(path, 0755)
			} else {
				log.Fatal(err)
			}
		}

		queryLoggers[databaseHash] = &QueryLog{
			branchUuid:     branchUuid,
			databaseUuid:   databaseUuid,
			mutex:          sync.Mutex{},
			path:           path,
			queryHasher:    crc64.New(crc64.MakeTable(crc64.ISO)),
			statementIndex: statementIndex,
			timestamp:      timestamp.Unix(),
			writeBuffer:    make(chan []byte, QueryLogBufferSize),
		}
	}

	// If the date has changed, close the current log file and set the timestamp
	// to the current date.
	if queryLoggers[databaseHash].timestamp != timestamp.Unix() {
		queryLoggers[databaseHash].timestamp = timestamp.Unix()
		queryLoggers[databaseHash].file.Close()
		queryLoggers[databaseHash].file = nil
	}

	return queryLoggers[databaseHash]
}

func Query(entry QueryLogEnry) error {
	log := GetQueryLog(
		entry.DatabaseHash,
		entry.DatabaseUuid,
		entry.BranchUuid,
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
		path := fmt.Sprintf("%s/%d/QUERY_LOG_%s", q.path, q.timestamp, node.Node().Id)

		err := storage.FS().MkdirAll(filepath.Dir(path), 0755)

		if err != nil {
			log.Fatal(err)
		}

		file, err := storage.FS().OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

		if err != nil {
			log.Fatal(err)
		}

		q.file = file
	}

	return q.file
}

func (q *QueryLog) Flush() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	file := q.GetFile()

	for len(q.writeBuffer) > 0 {
		data := <-q.writeBuffer

		_, err := file.Write(data)

		if err != nil {
			log.Fatal(err)
		}
	}

	err := file.Sync()

	if err != nil {
		log.Fatal(err)
	}
}

func (q *QueryLog) Read(start, end int) []map[string]interface{} {
	queryLogs := make([]map[string]interface{}, 0)
	timeInstance := time.Unix(int64(start), 0)
	startOfDay := int(time.Date(timeInstance.Year(), timeInstance.Month(), timeInstance.Day(), 0, 0, 0, 0, time.UTC).Unix())

	// Get all the directories in the logs directory
	path := fmt.Sprintf(
		"%s/logs",
		file.GetDatabaseFileBaseDir(q.databaseUuid, q.branchUuid),
	)

	dirs, err := storage.FS().ReadDir(path)

	if err != nil {
		if os.IsNotExist(err) {
			return queryLogs
		}

		log.Fatal(err)
	}

	for directory := range dirs {
		if !dirs[directory].IsDir {
			continue
		}

		// Get the timestamp of the directory
		directoryTimestamp, err := strconv.Atoi(dirs[directory].Name)

		if err != nil {
			log.Fatal(err)
		}

		// If the timestamp is not in the range, skip it
		if directoryTimestamp < startOfDay || directoryTimestamp > end {
			continue
		}

		file, err := storage.FS().Open(fmt.Sprintf("%s/%d/%s", path, directoryTimestamp, "query.log"))

		if err != nil && os.IsNotExist(err) {
			continue
		} else if err != nil {
			log.Fatal(err)
		}

		defer file.Close()

		// Read all the lines of the file
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()

			// Read the timestamp at the start of the line that is wrapped in brackets
			regexp := regexp.MustCompile(`\[(.*?)\]`)
			matches := regexp.FindStringSubmatch(line)

			if len(matches) == 0 {
				continue
			}

			timestamp, err := strconv.Atoi(matches[1])

			if err != nil {
				log.Fatal(err)
			}

			// if the timstamp is not in the range continue
			if timestamp < start || timestamp > end {
				continue
			}

			// Read the JSON data at the end of the line
			jsonString := strings.Trim(strings.Split(line, matches[0])[1], " ")

			var data map[string]interface{}

			err = json.Unmarshal([]byte(jsonString), &data)

			if err != nil {
				log.Fatal(err)
			}

			// Add the data to the array
			queryLogs = append(queryLogs, data)
		}
	}

	return queryLogs
}

func (q *QueryLog) Watch() {
	if q.watching {
		return
	}

	q.watching = true

	go func() {
		ticker := time.NewTicker(3 * time.Second)

		for {
			select {
			case <-node.Node().Context().Done():
				return
			case <-ticker.C:
				q.watching = false
				ticker.Stop()
				q.Flush()
				return
			default:
				if len(q.writeBuffer) == QueryLogBufferSize {
					ticker.Reset(3 * time.Second)
					q.Flush()
				}

				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (q *QueryLog) Write(accessKeyId, statement string, latency float64) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	timestamp := time.Now().UTC().Unix()

	buffer := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(buffer)
	buffer.Reset()

	buffer.WriteString("access_key_id=")
	buffer.WriteString(accessKeyId)
	buffer.WriteString(" statement=")
	buffer.WriteString(strings.ToLower(statement))

	logData := buffer.Bytes()
	q.queryHasher.Reset()
	q.queryHasher.Write(logData)
	checksum := q.queryHasher.Sum64()
	// Convert the checksum to a hexadecimal
	key := strconv.FormatUint(checksum, 16)

	// Check if the statement is already in the dictionary
	if _, ok := q.statementIndex.Get(key); !ok {
		err := q.statementIndex.Set(key, string(logData))

		if err != nil {
			log.Fatal(err)
		}
	}

	data := queryLogBuffer.Get().(*bytes.Buffer)
	defer queryLogBuffer.Put(data)

	data.Reset() // Clear the buffer before use

	binary.Write(data, binary.LittleEndian, uint64(timestamp))
	binary.Write(data, binary.LittleEndian, math.Float64bits(latency))
	binary.Write(data, binary.LittleEndian, checksum)

	if !q.watching {
		go q.Watch()
	}

	q.writeBuffer <- data.Bytes()
}
