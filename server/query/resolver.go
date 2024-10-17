package query

import (
	"errors"
	"fmt"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/logs"
	"litebase/server/sqlite3"
	"log"
	"time"
)

type Resolver struct {
}

func ResolveQuery(query *Query, response *QueryResponse) error {
	if query.invalid {
		return fmt.Errorf("invalid or malformed query")
	}

	if shouldForwardToPrimary(query) {
		return forwardQueryToPrimary(query, response)
	}

	return resolveQueryLocally(query, response)
}

func resolveQueryLocally(query *Query, response *QueryResponse) error {
	return resolveWithQueue(query, response, func(query *Query, response *QueryResponse) error {
		start := time.Now()
		var sqlite3Result sqlite3.Result
		var statement database.Statement
		var changes int64
		var lastInsertRowID int64
		var err error
		var db *database.ClientConnection

		db, err = query.databaseManager.ConnectionManager().Get(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId)

		if err != nil {
			log.Println("Error getting database connection", err)

			return err
		}

		defer query.databaseManager.ConnectionManager().Release(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId, db)

		db = db.WithAccessKey(query.AccessKey)

		if query.IsPragma() {
			log.Println("Executing pragma query")
			sqlite3Result, err = db.GetConnection().SqliteConnection().Exec(db.GetConnection().Context(), query.Input.Statement)
			changes = db.GetConnection().Changes()
		} else {
			statement, err = db.GetConnection().Statement(query.Input.Statement)

			if err == nil {
				// err = query.Validate(statement)

				// if err != nil {
				// 	return QueryResponse{}, err
				// }

				sqlite3Result, err = db.GetConnection().Query(statement.Sqlite3Statement, query.Input.Parameters...)

				if !query.IsDQL() {
					changes = db.GetConnection().Changes()
					lastInsertRowID = db.GetConnection().SqliteConnection().LastInsertRowID()
				}
			}
		}

		if err != nil {
			query.databaseManager.ConnectionManager().Remove(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId, db)
			return err
		}

		response.Changes = changes
		response.Columns = sqlite3Result.Columns
		response.Id = query.Input.Id
		response.LastInsertRowId = lastInsertRowID
		response.Latency = float64(time.Since(start)) / float64(time.Millisecond)
		response.Rows = sqlite3Result.Rows
		response.RowCount = len(sqlite3Result.Rows)

		logs.Query(
			logs.QueryLogEnry{
				Cluster:      query.cluster,
				DatabaseHash: query.DatabaseKey.DatabaseHash,
				DatabaseId:   query.DatabaseKey.DatabaseId,
				BranchId:     query.DatabaseKey.BranchId,
				AccessKeyId:  query.AccessKey.AccessKeyId,
				Statement:    query.Input.Statement,
				Latency:      response.Latency,
			},
		)

		return err
	})
}

func resolveWithQueue(
	query *Query,
	response *QueryResponse,
	f func(query *Query, response *QueryResponse) error,
) error {
	if query.IsWrite() {
		queue := GetWriteQueue(query)

		if queue == nil {
			return fmt.Errorf("database not found")
		}

		return queue.Handle(
			func(f func(query *Query, response *QueryResponse) error,
				query *Query,
				response *QueryResponse,
			) error {
				return f(query, response)
			}, f, query, response)
	}

	return f(query, response)
}

func forwardQueryToPrimary(query *Query, response *QueryResponse) error {
	primaryResponse, err := query.cluster.Node().Send(
		cluster.NodeMessage{
			Id:   fmt.Sprintf("query:%s", query.Input.Id),
			Type: "QueryMessage",
			Data: cluster.QueryMessage{
				AccessKeyId: query.AccessKey.AccessKeyId,
				BranchId:    query.DatabaseKey.BranchId,
				DatabaseId:  query.DatabaseKey.DatabaseId,
				Id:          query.Input.Id,
				Statement:   query.Input.Statement,
				Parameters:  query.Input.Parameters,
			},
		},
	)

	if err != nil {
		log.Println("Error forwarding query to primary", err)
		return errors.New("error forwarding query to primary")
	}

	if primaryResponse.Type == "Error" {
		return fmt.Errorf(primaryResponse.Error)
	}

	if primaryResponse.Type != "QueryMessageResponse" {
		return fmt.Errorf("unexpected response from primary")
	}

	response.Changes = primaryResponse.Data.(cluster.QueryMessageResponse).Changes
	response.Columns = primaryResponse.Data.(cluster.QueryMessageResponse).Columns
	response.Latency = primaryResponse.Data.(cluster.QueryMessageResponse).Latency
	response.LastInsertRowId = primaryResponse.Data.(cluster.QueryMessageResponse).LastInsertRowID
	response.RowCount = primaryResponse.Data.(cluster.QueryMessageResponse).RowCount
	response.Rows = primaryResponse.Data.(cluster.QueryMessageResponse).Rows

	return nil
}

func shouldForwardToPrimary(query *Query) bool {
	return (query.IsPragma() || query.IsDML()) && query.cluster.Node().Membership != cluster.ClusterMembershipPrimary
}
