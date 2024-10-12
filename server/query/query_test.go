package query_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/database"
	"litebase/server/query"
	"testing"
)

func TestNewQuery(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		query, err := query.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			database.NewDatabaseKey(mock.DatabaseId, mock.BranchId),
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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		queryResponse := &query.QueryResponse{}
		query, err := query.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			database.NewDatabaseKey(mock.DatabaseId, mock.BranchId),
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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

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
// 	test.Run(t, func(app *server.App) {
// 		mock := test.MockDatabase(app)
// 		db, _ := database.Get(mock.DatabaseId, mock.BranchId, nil, false)

// 		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

// 		db, _ = database.Get(mock.DatabaseId, mock.BranchId, nil, false)

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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

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
