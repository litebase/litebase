package query

import (
	"fmt"
	"litebase/server/database"
	"litebase/server/sqlite3"
	"time"
)

type Resolver struct {
}

func ResolveQuery(db *database.ClientConnection, query Query) (map[string]interface{}, error) {
	// var handlerError error
	// var response map[string]interface{}

	var err error
	var data map[string]any

	if query.invalid {
		return map[string]any{
			"status":  "error",
			"message": fmt.Errorf("invalid or malformed query"),
		}, nil
	}

	start := time.Now()
	var sqlite3Result sqlite3.Result
	var statement database.Statement

	if query.IsPragma {
		sqlite3Result, err = db.GetConnection().SqliteConnection().Exec(query.OriginalStatement)
	} else {
		statement, err = query.Statement()

		if err == nil {

			err = query.Validate(statement)

			if err != nil {
				return map[string]any{
					"status":  "error",
					"message": err.Error(),
				}, nil
			}

			sqlite3Result, err = db.GetConnection().Query(statement.Sqlite3Statement, query.Parameters()...)
		}

	}

	if err != nil {
		data = map[string]any{
			"status":  "error",
			"message": err.Error(),
		}
	} else {
		result := map[string]any{
			"changes":         db.GetConnection().Changes(),
			"lastInsertRowID": db.GetConnection().SqliteConnection().LastInsertRowID(),
			"rows":            sqlite3Result,
			"rowCount":        len(sqlite3Result),
		}

		data = map[string]any{
			"_execution_time": float64(time.Since(start)) / float64(time.Millisecond),
			"status":          "success",
			"data":            result,
		}

		// logging.Query(
		// 	clientConnection.GetDatabaseUuid(),
		// 	clientConnection.GetBranchUuid(),
		// 	query.AccessKeyId,
		// 	query.OriginalStatement,
		// 	float64(end)/float64(1000), // Convert to seconds
		// )
	}

	return data, err
}
