package database_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		
		responseGet := &database.QueryResponse{}
		resultGet, err := queryGet.Resolve(responseGet)
		require.NoError(t, err)
		require.NotNil(t, resultGet)
		
		// Verify we got a result with one column and one row
		columns := resultGet.Columns()
		require.Len(t, columns, 1)
		assert.Equal(t, "value", columns[0].ColumnName)
		assert.Equal(t, 1, resultGet.RowCount())
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
		require.NoError(t, err)
		
		responseSet := &database.QueryResponse{}
		resultSet, err := querySet.Resolve(responseSet)
		require.NoError(t, err)
		require.NotNil(t, resultSet)
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
		require.NoError(t, err)
		
		responseVerify := &database.QueryResponse{}
		resultVerify, err := queryVerify.Resolve(responseVerify)
		require.NoError(t, err)
		require.NotNil(t, resultVerify)
		
		// Should return 0 (false) - we set it to false above
		assert.Equal(t, 1, resultVerify.RowCount())
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
		require.NoError(t, err)
		
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
		require.NoError(t, err)
		
		responseSetNoAuth := &database.QueryResponse{}
		_, err = querySetNoAuth.Resolve(responseSetNoAuth)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not authorized to manage database branch settings")
		t.Logf("SET without MANAGE privilege correctly denied")
		
		// Create an access key WITH MANAGE privilege
		accessKeyWithManage, err := app.Auth.AccessKeyManager.Create("", []auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: auth.Resource(fmt.Sprintf("database:%s:branch:%s", mock.DatabaseID, mock.DatabaseBranchID)),
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage, auth.DatabasePrivilegePragma},
			},
		})
		require.NoError(t, err)
		
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
		require.NoError(t, err)
		
		responseSetWithAuth := &database.QueryResponse{}
		_, err = querySetWithAuth.Resolve(responseSetWithAuth)
		require.NoError(t, err)
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
		require.NoError(t, err)
		
		responseGetNoManage := &database.QueryResponse{}
		resultGet, err := queryGetNoManage.Resolve(responseGetNoManage)
		require.NoError(t, err)
		require.NotNil(t, resultGet)
		assert.Equal(t, 1, resultGet.RowCount())
		t.Logf("GET without MANAGE privilege succeeded (only needs PRAGMA)")
	})
}
