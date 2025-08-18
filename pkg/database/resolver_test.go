package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryResolver_Handle(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		db.GetConnection().Exec("CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		cases := []struct {
			statement  string
			parameters []sqlite3.StatementParameter
			expected   string
		}{
			{
				"SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"success",
			},
			{
				"SELECT * FROM users LIMIT ?",
				[]sqlite3.StatementParameter{
					{
						Type:  "INTEGER",
						Value: int64(1),
					},
				},
				"success",
			},
			{
				"?SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"error",
			},
		}

		queryResponse := &database.QueryResponse{}

		for _, c := range cases {
			q, err := database.NewQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				auth.NewDatabaseKey(mock.DatabaseID, mock.DatabaseName, mock.DatabaseBranchID, mock.BranchName),
				mock.Credential,
				&database.QueryInput{
					Statement:  c.statement,
					Parameters: c.parameters,
					ID:         "",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			queryResponse.Reset()

			_, err = q.Resolve(queryResponse)

			if err != nil && c.expected == `success` {
				t.Fatal(err)
			}
		}
	})
}
