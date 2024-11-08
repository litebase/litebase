package database

import (
	"errors"
	"fmt"
	"litebase/server/cluster/messages"
	"litebase/server/logs"
	"litebase/server/sqlite3"
	"log"
	"time"
)

func ResolveQuery(query *Query, response *QueryResponse) (*QueryResponse, error) {
	if query.invalid {
		return nil, fmt.Errorf("invalid or malformed query")
	}

	if shouldForwardToPrimary(query) {
		return forwardQueryToPrimary(query, response)
	}

	return resolveQueryLocally(query, response)
}

func resolveQueryLocally(query *Query, response *QueryResponse) (*QueryResponse, error) {
	return resolveWithQueue(query, response, func(query *Query, response *QueryResponse) (*QueryResponse, error) {
		start := time.Now()
		var sqlite3Result *sqlite3.Result
		var statement Statement
		var changes int64
		var lastInsertRowID int64
		var err error
		var db *ClientConnection

		db, err = query.databaseManager.ConnectionManager().Get(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId)

		if err != nil {
			log.Println("Error getting database connection", err)

			return response, err
		}

		defer query.databaseManager.ConnectionManager().Release(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId, db)

		db = db.WithAccessKey(query.AccessKey)

		if query.IsPragma() {
			sqlite3Result, err = db.GetConnection().SqliteConnection().Exec(db.GetConnection().Context(), query.Input.Statement)
			changes = db.GetConnection().Changes()
		} else {
			statement, err = db.GetConnection().Statement(query.Input.Statement)

			if err == nil {
				// err = query.Validate(statement)

				// if err != nil {
				// 	return QueryResponse{}, err
				// }

				sqlite3Result = db.GetConnection().ResultPool().Get()
				defer db.GetConnection().ResultPool().Put(sqlite3Result)

				sqlite3Result.Reset()

				err = db.GetConnection().Query(
					sqlite3Result,
					statement.Sqlite3Statement,
					query.Input.Parameters,
				)

				if !query.IsDQL() {
					changes = db.GetConnection().Changes()
					lastInsertRowID = db.GetConnection().SqliteConnection().LastInsertRowID()
				}
			}
		}

		if err != nil {
			query.databaseManager.ConnectionManager().Remove(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId, db)
			return response, err
		}

		response.SetChanges(changes)
		response.SetColumns(sqlite3Result.Columns)
		response.SetId(query.Input.Id)
		response.SetLastInsertRowId(lastInsertRowID)
		response.SetLatency(float64(time.Since(start)) / float64(time.Millisecond))
		response.SetRows(sqlite3Result.Rows)
		response.SetRowCount(len(sqlite3Result.Rows))

		logs.Query(
			logs.QueryLogEnry{
				Cluster:      query.cluster,
				DatabaseHash: query.DatabaseKey.DatabaseHash,
				DatabaseId:   query.DatabaseKey.DatabaseId,
				BranchId:     query.DatabaseKey.BranchId,
				AccessKeyId:  query.AccessKey.AccessKeyId,
				Statement:    query.Input.Statement,
				Latency:      response.Latency(),
			},
		)

		return response, err
	})
}

func resolveWithQueue(
	query *Query,
	response *QueryResponse,
	f func(query *Query, response *QueryResponse) (*QueryResponse, error),
) (*QueryResponse, error) {
	if query.IsWrite() {
		queue := query.databaseManager.WriteQueueManager.GetWriteQueue(query)

		if queue == nil {
			return nil, fmt.Errorf("database not found")
		}

		return queue.Handle(
			func(f func(query *Query, response *QueryResponse) (*QueryResponse, error),
				query *Query,
				response *QueryResponse,
			) (*QueryResponse, error) {
				return f(query, response)
			}, f, query, response)
	}

	return f(query, response)
}

func forwardQueryToPrimary(query *Query, response *QueryResponse) (*QueryResponse, error) {
	responseMessage, err := query.cluster.Node().Send(
		messages.NodeMessage{
			Data: messages.QueryMessage{
				AccessKeyId: query.AccessKey.AccessKeyId,
				BranchId:    query.DatabaseKey.BranchId,
				DatabaseId:  query.DatabaseKey.DatabaseId,
				ID:          query.Input.Id,
				Statement:   query.Input.Statement,
				Parameters:  query.Input.Parameters,
			},
		},
	)

	if err != nil {
		log.Println("Error forwarding query to primary", err)
		return nil, errors.New("error forwarding query to primary")
	}

	switch primaryResponse := responseMessage.(type) {
	case messages.ErrorMessage:
		return nil, fmt.Errorf(primaryResponse.Message)
	case messages.QueryMessageResponse:
		response.SetChanges(primaryResponse.Changes)
		response.SetColumns(primaryResponse.Columns)
		response.SetLatency(primaryResponse.Latency)
		response.SetLastInsertRowId(primaryResponse.LastInsertRowID)
		response.SetRowCount(primaryResponse.RowCount)
		response.SetRows(primaryResponse.Rows)

	default:
		return nil, fmt.Errorf("unexpected response from primary")
	}

	return nil, nil
}

func shouldForwardToPrimary(query *Query) bool {
	return !query.cluster.Node().IsPrimary() &&
		(query.IsPragma() || query.IsDML())
}
