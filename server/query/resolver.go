package query

import (
	"fmt"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/node"
	"litebase/server/sqlite3"
	"log"
	"time"
)

type Resolver struct {
}

func ResolveQuery(databaseHash string, query *Query) (QueryResponse, error) {
	if query.invalid {
		return QueryResponse{}, fmt.Errorf("invalid or malformed query")
	}

	if shouldForwardToPrimary(query) {
		return forwardQueryToPrimary(query.DatabaseUuid, query.BranchUuid, query)
	}

	return resolveQueryLocally(databaseHash, query.DatabaseUuid, query.BranchUuid, query)
}

func resolveQueryLocally(databaseHash, databaseUuid, branchUuid string, query *Query) (QueryResponse, error) {
	return resolveWithQueue(databaseHash, databaseUuid, branchUuid, query, func(query *Query) (QueryResponse, error) {
		var data QueryResponse
		start := time.Now()
		var sqlite3Result sqlite3.Result
		var statement database.Statement
		var changes int64
		var lastInsertRowID int64
		var err error
		var db *database.ClientConnection

		db, err = database.ConnectionManager().Get(databaseUuid, branchUuid)

		if err != nil {
			log.Println("Error getting database connection", err)

			return QueryResponse{
				Id: query.Id,
			}, err
		}

		defer database.ConnectionManager().Release(databaseUuid, branchUuid, db)

		db = db.WithAccessKey(query.AccessKey)

		if query.IsPragma() {
			log.Println("Executing pragma query")
			sqlite3Result, err = db.GetConnection().SqliteConnection().Exec(db.GetConnection().Context(), query.Statement)
			changes = db.GetConnection().Changes()
		} else {
			statement, err = db.GetConnection().Statement(query.Statement)

			if err == nil {
				// err = query.Validate(statement)

				// if err != nil {
				// 	return QueryResponse{}, err
				// }

				sqlite3Result, err = db.GetConnection().Query(statement.Sqlite3Statement, query.Parameters...)

				if !query.IsDQL() {
					changes = db.GetConnection().Changes()
					lastInsertRowID = db.GetConnection().SqliteConnection().LastInsertRowID()
				}
			}
		}

		if err != nil {
			database.ConnectionManager().Remove(databaseUuid, branchUuid, db)
			return QueryResponse{
				Id: query.Id,
			}, err
		}

		data = QueryResponse{
			Changes:         changes,
			Columns:         sqlite3Result.Columns,
			Id:              query.Id,
			LastInsertRowId: lastInsertRowID,
			Rows:            sqlite3Result.Rows,
			RowCount:        len(sqlite3Result.Rows),
		}

		data.ExecutionTime = float64(time.Since(start)) / float64(time.Millisecond)

		// logging.Query(
		// 	clientConnection.DatabaseUuid,
		// 	clientConnection.BranchUuid,
		// 	query.AccessKeyId,
		// 	query.OriginalStatement,
		// 	float64(end)/float64(1000), // Convert to seconds
		// )

		return data, err
	})
}

func resolveWithQueue(databaseHash, databaseUuid, branchUuid string, query *Query, f func(query *Query) (QueryResponse, error)) (QueryResponse, error) {
	if query.IsWrite() {
		queue := GetWriteQueue(databaseHash, databaseUuid, branchUuid)

		if queue == nil {
			return QueryResponse{}, fmt.Errorf("database not found")
		}

		return queue.Handle(func() (QueryResponse, error) {
			return f(query)
		})
	}

	return f(query)
}

func forwardQueryToPrimary(databaseUuid, branchUuid string, query *Query) (QueryResponse, error) {
	response, err := node.Node().Send(
		node.NodeMessage{
			Id:   fmt.Sprintf("query:%s", query.Id),
			Type: "QueryMessage",
			Data: node.QueryMessage{
				AccessKeyId:  query.AccessKey.AccessKeyId,
				BranchUuid:   branchUuid,
				DatabaseUuid: databaseUuid,
				Id:           query.Id,
				Statement:    query.Statement,
				Parameters:   query.Parameters,
			},
		},
	)

	if err != nil {
		return QueryResponse{}, err
	}

	if response.Type == "Error" {
		return QueryResponse{}, fmt.Errorf(response.Error)
	}

	if response.Type != "QueryMessageResponse" {
		return QueryResponse{}, fmt.Errorf("unexpected response from primary")
	}

	return QueryResponse{
		Changes:         response.Data.(node.QueryMessageResponse).Changes,
		Columns:         response.Data.(node.QueryMessageResponse).Columns,
		ExecutionTime:   response.Data.(node.QueryMessageResponse).ExecutionTime,
		LastInsertRowId: response.Data.(node.QueryMessageResponse).LastInsertRowID,
		RowCount:        response.Data.(node.QueryMessageResponse).RowCount,
		Rows:            response.Data.(node.QueryMessageResponse).Rows,
	}, nil
}

func shouldForwardToPrimary(query *Query) bool {
	return (query.IsPragma() || query.IsDML()) && node.Node().Membership != cluster.CLUSTER_MEMBERSHIP_PRIMARY
}
