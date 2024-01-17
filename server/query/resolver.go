package query

import (
	"fmt"
	db "litebasedb/server/database"
	"litebasedb/server/logging"
	"time"
)

type Resolver struct {
}

func NewResolver() *Resolver {
	return &Resolver{}
}

func (r *Resolver) Handle(db *db.ClientConnection, query *Query) map[string]interface{} {
	var handlerError error
	var response map[string]interface{}

	response, handlerError = r.resolveQuery(db, query)

	if handlerError != nil {
		fmt.Println("Error:", handlerError)
	}

	return response
}

func (r *Resolver) resolveQuery(
	clientConnection *db.ClientConnection,
	query *Query,
) (map[string]interface{}, error) {
	var err error
	var data map[string]any

	if query.invalid {
		return map[string]any{
			"status":  "error",
			"message": fmt.Errorf("invalid or malformed query"),
		}, nil
	}

	err = query.Validate()

	if err != nil {
		return map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, nil
	}

	start := time.Now()

	statement, err := query.Statement()

	if err != nil {
		return map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, nil
	}

	sqlite3Result, err := clientConnection.GetConnection().
		Query(statement, query.Parameters()...)

	end := float64(time.Since(start)) / float64(time.Millisecond)

	if err != nil {
		data = map[string]any{
			"status":  "error",
			"message": err.Error(),
		}
	} else {
		result := map[string]any{
			"changes":         clientConnection.GetConnection().Changes(),
			"lastInsertRowID": clientConnection.GetConnection().SqliteConnection().LastInsertRowID(),
			"rows":            sqlite3Result,
			"rowCount":        len(sqlite3Result),
		}

		data = map[string]any{
			"_execution_time": end,
			"status":          "success",
			"data":            result,
		}

		logging.Query(
			clientConnection.GetDatabaseUuid(),
			clientConnection.GetBranchUuid(),
			query.AccessKeyId,
			query.OriginalStatement,
			float64(end)/float64(1000), // Convert to seconds
		)
	}

	return data, err
}
