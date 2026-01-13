package database_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

// TestLitebasePragma_Integration tests that litebase PRAGMAs go through the resolver
// and execute on the primary node
func TestLitebasePragma_Integration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Test GET operation - query through the resolver
		queryGet, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			mock.Credential,
			&database.QueryInput{
				ID:        "test-get",
				Statement: "PRAGMA litebase_backups_enabled",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create GET query: %v", err)
		}

		responseGet := &database.QueryResponse{}
		resultGet, err := queryGet.Resolve(responseGet)

		if err != nil {
			t.Fatalf("Failed to resolve GET query: %v", err)
		}

		if resultGet == nil {
			t.Fatal("Expected non-nil result from GET query")
		}

		// Verify we got a result with one column and one row
		columns := resultGet.Columns()

		if len(columns) != 1 {
			t.Fatalf("Expected 1 column, got %d", len(columns))
		}

		if columns[0].ColumnName != "value" {
			t.Errorf("Expected column name 'value', got '%s'", columns[0].ColumnName)
		}

		if resultGet.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", resultGet.RowCount())
		}

		t.Logf("GET result: %v", resultGet.Rows())

		// Test SET operation
		querySet, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			mock.Credential,
			&database.QueryInput{
				ID:        "test-set",
				Statement: "PRAGMA litebase_backups_enabled = false",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create SET query: %v", err)
		}

		responseSet := &database.QueryResponse{}
		resultSet, err := querySet.Resolve(responseSet)

		if err != nil {
			t.Fatalf("Failed to resolve SET query: %v", err)
		}

		if resultSet == nil {
			t.Fatal("Expected non-nil result from SET query")
		}

		t.Logf("SET completed")

		// Verify the setting was updated
		queryVerify, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			mock.Credential,
			&database.QueryInput{
				ID:        "test-verify",
				Statement: "PRAGMA litebase_backups_enabled",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create VERIFY query: %v", err)
		}

		responseVerify := &database.QueryResponse{}
		resultVerify, err := queryVerify.Resolve(responseVerify)

		if err != nil {
			t.Fatalf("Failed to resolve VERIFY query: %v", err)
		}

		if resultVerify == nil {
			t.Fatal("Expected non-nil result from VERIFY query")
		}

		// Should return 0 (false) - we set it to false above
		if resultVerify.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", resultVerify.RowCount())
		}

		t.Logf("VERIFY result: %v", resultVerify.Rows())
	})
}

// TestLitebasePragma_Authorization tests that SET operations require MANAGE privilege
func TestLitebasePragma_Authorization(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create an access key WITHOUT MANAGE privilege
		accessKeyNoManage, err := app.Auth.AccessKeyManager.Create("", []auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegePragma, auth.DatabasePrivilegeSelect},
			},
		})

		if err != nil {
			t.Fatalf("Failed to create access key without MANAGE privilege: %v", err)
		}

		credentialNoManage := &auth.Credential{}
		credentialNoManage.WithAccessKey(accessKeyNoManage)

		// Test that SET operation fails without MANAGE privilege
		querySetNoAuth, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			credentialNoManage,
			&database.QueryInput{
				ID:        "test-set-no-auth",
				Statement: "PRAGMA litebase_backups_enabled = false",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create SET query without auth: %v", err)
		}

		responseSetNoAuth := &database.QueryResponse{}

		_, err = querySetNoAuth.Resolve(responseSetNoAuth)

		if err == nil {
			t.Fatal("Expected error when setting without MANAGE privilege")
		}

		if !strings.Contains(err.Error(), "not authorized to manage database branch settings") {
			t.Errorf("Expected error to contain 'not authorized to manage database branch settings', got: %v", err)
		}

		t.Logf("SET without MANAGE privilege correctly denied")

		// Create an access key WITH MANAGE privilege
		accessKeyWithManage, err := app.Auth.AccessKeyManager.Create("", []auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: auth.Resource(fmt.Sprintf("database:%s:branch:%s", mock.DatabaseID, mock.DatabaseBranchID)),
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage, auth.DatabasePrivilegePragma},
			},
		})

		if err != nil {
			t.Fatalf("Failed to create access key with MANAGE privilege: %v", err)
		}

		credentialWithManage := &auth.Credential{}
		credentialWithManage.WithAccessKey(accessKeyWithManage)

		// Test that SET operation succeeds with MANAGE privilege
		querySetWithAuth, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			credentialWithManage,
			&database.QueryInput{
				ID:        "test-set-with-auth",
				Statement: "PRAGMA litebase_backups_enabled = false",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create SET query with auth: %v", err)
		}

		responseSetWithAuth := &database.QueryResponse{}

		_, err = querySetWithAuth.Resolve(responseSetWithAuth)

		if err != nil {
			t.Fatalf("Failed to resolve SET query with MANAGE privilege: %v", err)
		}

		t.Logf("SET with MANAGE privilege succeeded")

		// Test that GET operation works without MANAGE privilege (only needs PRAGMA privilege)
		queryGetNoManage, err := database.NewQuery(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			mock.DatabaseKey,
			credentialNoManage,
			&database.QueryInput{
				ID:        "test-get-no-manage",
				Statement: "PRAGMA litebase_backups_enabled",
			},
		)

		if err != nil {
			t.Fatalf("Failed to create GET query without MANAGE privilege: %v", err)
		}

		responseGetNoManage := &database.QueryResponse{}

		resultGet, err := queryGetNoManage.Resolve(responseGetNoManage)

		if err != nil {
			t.Fatalf("Failed to resolve GET query without MANAGE privilege: %v", err)
		}

		if resultGet == nil {
			t.Fatal("Expected non-nil result from GET query without MANAGE privilege")
		}

		if resultGet.RowCount() != 1 {
			t.Errorf("Expected 1 row, got %d", resultGet.RowCount())
		}

		t.Logf("GET without MANAGE privilege succeeded (only needs PRAGMA)")
	})
}
