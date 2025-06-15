package logs_test

import (
	"fmt"
	"hash/crc64"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/server"
)

func TestQueryLog_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		err := l.Close()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueryLog_GetFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		file := l.GetFile()

		if file == nil {
			t.Fatal("File is nil")
		}
	})
}

func TestQueryLog_GetStatementIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		index, err := l.GetStatementIndex()

		if err != nil {
			t.Fatal(err)
		}

		if index == nil {
			t.Fatal("Statement index is nil")
		}
	})
}

func TestQueryLog_Flush(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		logs.QueryLogFlushInterval = time.Millisecond * 1
		logs.QueryLogFlushThreshold = time.Millisecond

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		l.Flush(true)

		fileInfo, err := l.GetFile().Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Ensure data was written to the  file
		if fileInfo.Size() != 0 {
			t.Fatal("File size should be 0")
		}

		err = l.Write(
			db.AccessKey.AccessKeyId,
			[]byte("SELECT * FROM test"),
			0.01,
		)

		if err != nil {
			t.Fatal(err)
		}

		l.Flush(true)

		fileInfo, err = l.GetFile().Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Ensure data was written to the  file
		if fileInfo.Size() == 0 {
			t.Fatal("File size should not be 0")
		}
	})
}

func TestQueryLog_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		logs.QueryLogFlushInterval = time.Millisecond * 1
		logs.QueryLogFlushThreshold = time.Millisecond

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		startTime := time.Now().UTC().Truncate(time.Second)

		l.Write(
			db.AccessKey.AccessKeyId,
			[]byte("SELECT * FROM test"),
			0.01,
		)

		l.Flush(true)

		endTime := time.Now().UTC().Truncate(time.Second)

		queryMetrics := l.Read(
			uint32(startTime.UTC().Unix()),
			uint32(endTime.UTC().Unix()),
		)

		if queryMetrics == nil {
			t.Fatal("Query metrics is nil")
		}

		if len(queryMetrics) != 1 {
			t.Fatal("Query metrics is empty")
		}

		hash64 := crc64.New(crc64.MakeTable(crc64.ISO))

		hash64.Write(fmt.Appendf(nil, "access_key_id=%s statement=select * from test", db.AccessKey.AccessKeyId))

		if queryMetrics[0].Checksum != hash64.Sum64() {
			t.Fatal("Query metrics checksum is incorrect")
		}
	})
}

func TestQueryLog_Write(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		err := l.Write(
			db.AccessKey.AccessKeyId,
			[]byte("SELECT * FROM test"),
			0.01,
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}
