package query

import (
	"encoding/json"
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/concurrency"
	db "litebasedb/runtime/database"
	"litebasedb/runtime/logging"
	"time"
)

type Resolver struct {
}

func NewResolver() *Resolver {
	return &Resolver{}
}

func (r *Resolver) Handle(db *db.Database, query *Query, ephemeral bool) map[string]interface{} {
	var handlerError error
	var response map[string]interface{}
	shouldLock := r.shouldLock(query)

	if shouldLock {
		concurrency.Lock()
	}

	response, handlerError = r.resolve(db, query)

	if handlerError != nil {
		fmt.Println("Error:", handlerError)
	}

	// Block until changes are replicated
	db.GetConnection().Operator.Transmit()

	if !ephemeral {
		db.GetConnection().Operator.Record()
	}

	db.Close()
	concurrency.Unlock()

	return response
}

func (r *Resolver) resolveQuery(database *db.Database, query *Query) (map[string]interface{}, error) {
	var err error
	var data map[string]any

	if query.Invalid {
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

	sqlite3Result, err := database.GetConnection().Query(statement, query.Parameters()...)
	end := time.Since(start).Microseconds()

	if err != nil {
		data = map[string]any{
			"status":  "error",
			"message": err.Error(),
		}
	} else {
		result := map[string]any{
			"changes":         database.GetConnection().Changes(),
			"lastInsertRowID": database.GetConnection().LastInsertRowID(),
			"rows":            sqlite3Result,
			"rowCount":        len(sqlite3Result),
		}

		queryData, err := json.Marshal(result)

		if err != nil {
			return nil, err
		}

		encryptedQueryData, err := auth.SecretsManager().EncryptFor(query.AccessKeyId, string(queryData))

		if err != nil {
			return nil, err
		}

		data = map[string]any{
			"_execution_time": end,
			"status":          "success",
			"data":            encryptedQueryData,
		}

		logging.Query(
			database.GetDatabaseUuid(),
			database.GetBranchUuid(),
			query.AccessKeyId,
			query.JsonStatement,
			float64(end)/float64(1000),
		)
	}

	return data, err
}

func (r *Resolver) resolve(database *db.Database, query *Query) (map[string]interface{}, error) {
	var handlerError error
	var response map[string]interface{}

	database.GetConnection().Begin()

	if len(query.Batch) > 0 {
		results := make([]any, 0)

		for _, query := range query.Batch {
			response, _ = r.resolveQuery(database, query)
			jsonResponse, err := json.Marshal(response)

			if err != nil {
				handlerError = err
			}

			results = append(results, json.RawMessage(string(jsonResponse)))
		}

		response = map[string]any{
			"status": "success",
			"data":   results,
		}
	} else {
		response, handlerError = r.resolveQuery(database, query)
	}

	database.GetConnection().Commit()

	return response, handlerError
}

func (r *Resolver) shouldLock(query *Query) bool {
	if len(query.Batch) > 0 {
		for _, query := range query.Batch {
			statement, err := query.Statement()

			if err != nil {
				return false
			}

			if !statement.IsReadonly() {
				return true
			}
		}

		return false
	}

	statement, err := query.Statement()

	if err != nil {
		return false
	}

	return !statement.IsReadonly()
}
