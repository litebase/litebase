package logging

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/file"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type QueryLog struct {
	branchUuid   string
	databaseUuid string
	file         *os.File
	path         string
}

var queryLoggers = make(map[string]*QueryLog)

func GetQueryLog(databaseUuid string, branchUuid string) *QueryLog {
	key := fmt.Sprintf("%s:%s", databaseUuid, branchUuid)

	if _, ok := queryLoggers[key]; !ok {
		queryLoggers[key] = &QueryLog{
			branchUuid:   branchUuid,
			databaseUuid: databaseUuid,
		}
	}

	return queryLoggers[key]
}

func Query(databaseUuid, branchUuid, accessKeyId, statement string, isWrite bool, executionTime float64) error {
	GetQueryLog(databaseUuid, branchUuid).Write(accessKeyId, statement, isWrite, executionTime)

	return nil
}

func (q *QueryLog) GetFile() *os.File {
	t := time.Now().UTC()
	timestamp := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	path := fmt.Sprintf(
		"%s/logs/%d/query.log",
		file.GetFileDir(q.databaseUuid, q.branchUuid),
		int(timestamp.Unix()),
	)

	if path != q.path {
		err := os.MkdirAll(filepath.Dir(path), 0755)

		if err != nil {
			panic(err)
		}

		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			panic(err)
		}

		q.file = file
	}

	return q.file
}

func (q *QueryLog) Read(start, end int) []map[string]interface{} {
	queryLogs := make([]map[string]interface{}, 0)
	timeInstance := time.Unix(int64(start), 0)
	startOfDay := int(time.Date(timeInstance.Year(), timeInstance.Month(), timeInstance.Day(), 0, 0, 0, 0, time.UTC).Unix())

	// Get all the directories in the logs directory
	path := fmt.Sprintf(
		"%s/logs",
		file.GetFileDir(q.databaseUuid, q.branchUuid),
	)

	dirs, err := os.ReadDir(path)

	if err != nil {
		panic(err)
	}
	for directory := range dirs {
		if !dirs[directory].IsDir() {
			continue
		}

		// Get the timestamp of the directory
		directoryTimestamp, err := strconv.Atoi(dirs[directory].Name())

		if err != nil {
			panic(err)
		}

		// If the timestamp is not in the range, skip it
		if directoryTimestamp < startOfDay || directoryTimestamp > end {
			continue
		}

		file, err := os.Open(fmt.Sprintf("%s/%d/%s", path, directoryTimestamp, "query.log"))

		if err != nil && os.IsNotExist(err) {
			continue
		} else if err != nil {
			panic(err)
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
				panic(err)
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
				panic(err)
			}

			// Add the data to the array
			queryLogs = append(queryLogs, data)
		}
	}

	return queryLogs
}

func (q *QueryLog) Write(accessKeyId, statement string, isWrite bool, executionTime float64) {
	timestamp := time.Now().UTC().Unix()

	log := map[string]interface{}{
		"accessKeyId":   accessKeyId,
		"executionTime": executionTime,
		"hash":          fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s:%s", accessKeyId, statement)))),
		"isWrite":       isWrite,
		"loggedAt":      timestamp,
		"statement":     statement,
	}

	jsonLog, err := json.Marshal(log)

	if err != nil {
		panic(err)
	}

	q.GetFile().WriteString(
		fmt.Sprintf("[%d] %s\n", timestamp, string(jsonLog)),
	)
}
