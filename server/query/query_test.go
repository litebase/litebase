package query_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"litebase/server/query"
	"testing"
)

func TestNewQuery(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		query, err := query.NewQuery(
			database.NewDatabaseKey(mock.DatabaseUuid, mock.BranchUuid),
			mock.AccessKey,
			&query.QueryInput{
				Statement:  "SELECT * FROM users LIMIT ?",
				Parameters: []interface{}{1},
				Id:         "",
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		if query.Input.Statement != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

func TestResolve(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, _ := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		queryResponse := &query.QueryResponse{}
		query, err := query.NewQuery(
			database.NewDatabaseKey(mock.DatabaseUuid, mock.BranchUuid),
			mock.AccessKey,
			&query.QueryInput{
				Statement:  "SELECT * FROM users LIMIT ?",
				Parameters: []interface{}{1},
				Id:         "",
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		err = query.Resolve(queryResponse)

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
			Input: &query.QueryInput{
				Statement:  "SELECT * FROM users LIMIT ?",
				Parameters: []interface{}{1},
			},
		}

		statement, err := db.GetConnection().Statement(query.Input.Statement)

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
			Input: &query.QueryInput{
				Statement:  "SELECT * FROM users LIMIT ?",
				Parameters: []interface{}{1},
			},
		}

		stmt, err := db.GetConnection().Statement(query.Input.Statement)

		if err != nil {
			t.Fatal(err)
		}

		err = query.Validate(stmt)

		if err != nil {
			t.Fatal(err)
		}
	})
}
