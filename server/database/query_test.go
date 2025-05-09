package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/sqlite3"
)

func TestNewQuery(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		query, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			auth.NewDatabaseKey(mock.DatabaseId, mock.BranchId),
			mock.AccessKey,
			&database.QueryInput{
				Statement: []byte("SELECT * FROM users LIMIT ?"),
				Parameters: []sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(1),
				}},
				Id: []byte(""),
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		if string(query.Input.Statement) != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

func TestResolve(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, []byte("CREATE TABLE users (id INT, name TEXT)"), []sqlite3.StatementParameter{})

		queryResponse := &database.QueryResponse{}
		query, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			auth.NewDatabaseKey(mock.DatabaseId, mock.BranchId),
			mock.AccessKey,
			&database.QueryInput{
				Statement: []byte("SELECT * FROM users LIMIT ?"),
				Parameters: []sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(1),
				}},
				Id: []byte(""),
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		_, err = query.Resolve(queryResponse)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestStatement(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, []byte("CREATE TABLE users (id INT, name TEXT)"), []sqlite3.StatementParameter{})

		db, _ = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		query := &database.Query{
			Input: &database.QueryInput{
				Statement: []byte("SELECT * FROM users LIMIT ?"),
				Parameters: []sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(1),
				}},
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
// 	test.RunWithApp(t, func(app *server.App) {
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
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, []byte("CREATE TABLE users (id INT, name TEXT)"), []sqlite3.StatementParameter{})

		db, _ = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		query := &database.Query{
			Input: &database.QueryInput{
				Statement: []byte("SELECT * FROM users LIMIT ?"),
				Parameters: []sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: int64(1),
				}},
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
