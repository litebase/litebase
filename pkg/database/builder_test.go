package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryBuilder(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		queryBuilder := database.NewQueryBuilder(
			app.Cluster,
			app.Auth,
			app.DatabaseManager,
			app.LogManager,
		)

		mock := test.MockDatabase(app)

		t.Run("Build", func(t *testing.T) {
			query, err := queryBuilder.Build(
				mock.Credential.CredentialID,
				mock.Credential.Scheme,
				mock.DatabaseID,
				mock.DatabaseName,
				mock.DatabaseBranchID,
				mock.BranchName,
				"SELECT * FROM mock_table",
				[]sqlite3.StatementParameter{},
				"mock-id",
			)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if query == nil {
				t.Fatal("expected query to be non-nil")
			}
		})
	})
}
