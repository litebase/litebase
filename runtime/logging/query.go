package logging

import (
	"encoding/json"
	"fmt"
	"litebasedb/runtime/file"
	"os"
	"path/filepath"
	"time"
)

type QueryLog struct {
	branchUuid   string
	databaseUuid string
	file         *os.File
	path         string
}

var queryLoggers = make(map[string]*QueryLog)

func Query(databaseUuid, branchUuid, accessKeyId, statement string, executionTime float64) error {
	key := fmt.Sprintf("%s:%s", databaseUuid, branchUuid)

	if _, ok := queryLoggers[key]; !ok {
		queryLoggers[key] = &QueryLog{
			databaseUuid: databaseUuid,
			branchUuid:   branchUuid,
		}
	}

	queryLoggers[key].Write(accessKeyId, statement, executionTime)

	return nil
}

func (q *QueryLog) GetFile() *os.File {
	t := time.Now()
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

func (q *QueryLog) Write(accessKeyId, statement string, executionTime float64) {
	timestamp := time.Now().UTC().Format(time.RFC3339)

	log := map[string]interface{}{
		"accessKeyId":   accessKeyId,
		"executionTime": executionTime,
		"loggedAt":      timestamp,
		"statement":     statement,
	}

	jsonLog, err := json.Marshal(log)

	if err != nil {
		panic(err)
	}

	q.GetFile().WriteString(
		fmt.Sprintf("%s: %s\n", timestamp, string(jsonLog)),
	)
}
