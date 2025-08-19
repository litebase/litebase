package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestTransaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		t.Run("NewTransaction", func(t *testing.T) {
			transaction, err := database.NewTransaction(
				app.Cluster,
				app.DatabaseManager,
				mock.DatabaseKey,
				mock.Credential,
			)

			if err != nil {
				t.Fatal("Failed to create new transaction:", err)
			}

			defer transaction.Close()
		})

		t.Run("CloseTransaction", func(t *testing.T) {
			transaction, err := database.NewTransaction(
				app.Cluster,
				app.DatabaseManager,
				mock.DatabaseKey,
				mock.Credential,
			)

			if err != nil {
				t.Fatal("Failed to create new transaction:", err)
			}

			err = transaction.Close()

			if err != nil {
				t.Fatal("Failed to close transaction:", err)
			}
		})

		t.Run("CommitTransaction", func(t *testing.T) {
			transaction, err := database.NewTransaction(
				app.Cluster,
				app.DatabaseManager,
				mock.DatabaseKey,
				mock.Credential,
			)

			if err != nil {
				t.Fatal("Failed to create new transaction:", err)
			}

			defer transaction.Close()

			err = transaction.ResolveQuery(database.GetQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				mock.DatabaseKey,
				mock.Credential,
				&database.QueryInput{
					Statement: "CREATE TABLE users_commit (id SERIAL PRIMARY KEY, name TEXT)",
				},
			), &database.QueryResponse{})

			if err != nil {
				t.Fatal("Failed to resolve query:", err)
			}

			if err := transaction.Commit(); err != nil {
				t.Fatal("Failed to commit transaction:", err)
			}
		})

		t.Run("RollbackTransaction", func(t *testing.T) {
			transaction, err := database.NewTransaction(
				app.Cluster,
				app.DatabaseManager,
				mock.DatabaseKey,
				mock.Credential,
			)

			if err != nil {
				t.Fatal("Failed to create new transaction:", err)
			}

			defer transaction.Close()

			err = transaction.ResolveQuery(database.GetQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				mock.DatabaseKey,
				mock.Credential,
				&database.QueryInput{
					Statement: "CREATE TABLE users_rollback (id SERIAL PRIMARY KEY, name TEXT)",
				},
			), &database.QueryResponse{})

			if err != nil {
				t.Fatal("Failed to resolve query:", err)
			}

			if err := transaction.Rollback(); err != nil {
				t.Fatal("Failed to rollback transaction:", err)
			}
		})
	})
}
