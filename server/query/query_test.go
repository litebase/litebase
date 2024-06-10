package query_test

import (
	"litebasedb/internal/test"
	"litebasedb/server/database"
	"litebasedb/server/query"
	"log"
	"testing"
)

func TestNewQuery(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			log.Fatal(err)
		}

		query, err := query.NewQuery(
			db, mock.AccessKeyId, map[string]interface{}{
				"statement":  "SELECT * FROM users LIMIT ?",
				"parameters": []interface{}{1},
			},
			"",
		)

		if err != nil {
			t.Fatal(err)
		}

		if query.OriginalStatement != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

func TestResolve(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, _ := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		query, err := query.NewQuery(
			db,
			mock.AccessKeyId,
			map[string]interface{}{
				"statement":  "SELECT * FROM users LIMIT ?",
				"parameters": []interface{}{1},
			},
			"",
		)

		if err != nil {
			t.Fatal(err)
		}

		response, err := query.Resolve()

		if response["status"] != "success" {
			t.Fatal("Response status is not correct")
		}

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestStatement(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, _ := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		query := &query.Query{
			ClientConnection:   db,
			OriginalStatement:  "SELECT * FROM users LIMIT ?",
			OriginalParameters: []interface{}{1},
		}

		statement, err := query.Statement()

		if err != nil {
			t.Fatal(err)
		}

		if statement.Sqlite3Statement.SQL() != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

// func TestStatementOfBatchQuery(t *testing.T) {
// 	test.Run(t, func() {
// 		mock := test.MockDatabase()
// 		db, _ := database.Get(mock.DatabaseUuid, mock.BranchUuid, nil, false)

// 		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

// 		db, _ = database.Get(mock.DatabaseUuid, mock.BranchUuid, nil, false)

// 		query := &Query{
// 			Batch: []*Query{{
// 				Database:           db,
// 				OriginalStatement:  "SELECT * FROM users LIMIT ?",
// 				OriginalParameters: "[1]",
// 			}},
// 			Database: db,
// 		}

// 		statement, err := query.Statement()

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		if statement != nil {
// 			t.Fatal("Statement should be nil for a query with the batch field")
// 		}
// 	})
// }

func TestValidate(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, _ := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		query := &query.Query{
			ClientConnection:   db,
			OriginalStatement:  "SELECT * FROM users LIMIT ?",
			OriginalParameters: []interface{}{1},
		}

		stmt, err := db.GetConnection().Statement(query.OriginalStatement)

		if err != nil {
			t.Fatal(err)
		}

		err = query.Validate(stmt)

		if err != nil {
			t.Fatal(err)
		}
	})
}
