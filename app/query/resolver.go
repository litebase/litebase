package query

import (
	"encoding/json"
	"fmt"
	"litebasedb/runtime/app/concurrency"
	db "litebasedb/runtime/app/database"
	"os"
	"strings"
	"time"
)

type Resolver struct {
}

func NewResolver() *Resolver {
	return &Resolver{}
}

func (r *Resolver) Handle(database *db.Database, query Query) []byte {
	var handlerError error
	var response []byte
	shouldLock := r.ShouldLock(query)
	handlerStart := time.Now()

	if shouldLock {
		concurrency.Lock()
	}

	response, handlerError = r.resolve(database, handlerStart, query)

	// Block until changes are replicated
	// database.Operator.Transmit()

	if handlerError != nil {
		fmt.Println("Error:", handlerError)
	}

	database.Close()
	concurrency.Unlock()
	fmt.Printf("Handler took: %s \n", time.Since(handlerStart))

	return response
}

func (r *Resolver) resolveQuery(database *db.Database, handlerStart time.Time, query Query) ([]byte, error) {
	var response []byte
	var err error
	var data map[string]any

	sqlite3Result, err := database.GetConnection().Query(query.Statement, query.Parameters...)

	if err != nil {
		data = map[string]any{
			"status":  "error",
			"message": err.Error(),
		}
	} else {
		data = map[string]any{
			"id":   os.Getenv("LITEBASEDB_RUNTIME_ID"),
			"time": time.Since(handlerStart).String(),
			"data": map[string]any{
				"changes":         database.GetConnection().Changes(),
				"lastInsertRowID": database.GetConnection().LastInsertRowID(),
				"rows":            sqlite3Result,
			},
		}
	}

	response, err = json.Marshal(data)

	return response, err
}

func (r *Resolver) resolve(database *db.Database, handlerStart time.Time, query Query) ([]byte, error) {
	var handlerError error
	var response []byte

	database.GetConnection().Begin()

	if len(query.Batch) > 0 {
		results := make([]any, 0)

		for _, query := range query.Batch {
			response, _ = r.resolveQuery(database, handlerStart, query)
			results = append(results, json.RawMessage(string(response)))
		}

		response, handlerError = json.Marshal(map[string]any{
			"id":   os.Getenv("LITEBASEDB_RUNTIME_ID"),
			"time": time.Since(handlerStart).String(),
			"data": results,
		})
	} else {
		response, handlerError = r.resolveQuery(database, handlerStart, query)
	}

	database.GetConnection().Commit()

	return response, handlerError
}

func (r *Resolver) ShouldLock(query Query) bool {
	if len(query.Batch) > 0 {
		for _, query := range query.Batch {
			if !strings.HasPrefix(query.Statement, "SELECT") {
				return true
			}
		}

		return false
	}

	return !strings.HasPrefix(query.Statement, "SELECT")
}
