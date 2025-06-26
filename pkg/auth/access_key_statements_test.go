package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestAccessKeyAuthorizes(t *testing.T) {
	testCases := []struct {
		result     bool
		resource   string
		statements []auth.AccessKeyStatement
	}{
		{
			result:   true,
			resource: "*",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   false,
			resource: "*",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
				{
					Effect:   auth.AccessKeyEffectDeny,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   false,
			resource: "*",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectDeny,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   false,
			resource: "*",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
				{
					Effect:   auth.AccessKeyEffectDeny,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   true,
			resource: "database:x",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "database:x",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   true,
			resource: "database:x",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "database:*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   true,
			resource: "database:x:table:y",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "database:x:table:*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   true,
			resource: "database:x:table:y",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "database:x:table:y",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
		{
			result:   true,
			resource: "database:x:table:y",
			statements: []auth.AccessKeyStatement{
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
				{
					Effect:   auth.AccessKeyEffectAllow,
					Resource: "database:x:table:y:*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
				},
			},
		},
	}

	accessKey := &auth.AccessKey{}

	for _, testCase := range testCases {
		accessKey.Statements = testCase.statements

		if auth.Authorized(accessKey.Statements, testCase.resource, auth.DatabasePrivilegeRead) != testCase.result {
			t.Errorf("Expected accessKey to be %v for resource %q", testCase.result, testCase.resource)
		}
	}
}

func TestAccessKeyStatements(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CanAccessDatabase", func(t *testing.T) {
			db := test.MockDatabase(app)

			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"",
				[]auth.AccessKeyStatement{
					{
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
			)

			if err := accessKey.CanAccessDatabase("", ""); err == nil {
				t.Error("Expected accessKey to not have access to database")
			}

			if err := accessKey.CanAccessDatabase(db.DatabaseId, db.BranchId); err != nil {
				t.Error("Expected accessKey to have access to database")
			}
		})

		t.Run("CanAnalyze", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name:       "(*|*|allow)",
					args:       []string{"test_table"},
					statements: []auth.AccessKeyStatement{{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}}},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAnalyze),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}}},
				},
				{
					name:          "(database:DATABASE_ID:*|ANALYZE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|ANALYZE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAnalyze),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|ANALYZE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|ANALYZE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAnalyze),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME:*|ANALYZE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:%s:*", db.DatabaseId, db.BranchId, "test_table")), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:table:*|ANALYZE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAnalyze),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAnalyze}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanAnalyze(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Fatalf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Fatalf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("ANALYZE test_table", nil)

					if err != nil && testCase.expectedError == nil {
						t.Fatalf("Error analyzing table: %v", err)
					}
				})
			}
		})

		t.Run("CanAttach", func(t *testing.T) {
			db := test.MockDatabase(app)

			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"",
				[]auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
			)

			// Test the access key permissions directly
			check, err := accessKey.CanAttach(
				db.DatabaseId,
				db.BranchId,
				"test_table",
			)

			if err == nil {
				t.Fatalf("Expected error when checking attach permissions")
			}

			if check {
				t.Fatalf("Should not have attach permissions by default")
			}
		})

		t.Run("CanAlterTable", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name:       "(*|*|allow)",
					args:       []string{"main", "test_table"},
					statements: []auth.AccessKeyStatement{{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}}},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(*|ALTER_TABLE|allow)",
					args:          []string{"main", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(*|ALTER_TABLE|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|ALTER_TABLE|allow)",
					args:          []string{"main", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|ALTER_TABLE|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:table:BRANCH_ID:*|ALTER_TABLE|allow)",
					args:          []string{"main", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|ALTER_TABLE|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:table:BRANCH_ID:table:*|ALTER_TABLE|allow)",
					args:          []string{"main", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:table:*|ALTER_TABLE|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:table:BRANCH_ID:table:TABLE_NAME|ALTER_TABLE|allow)",
					args:          []string{"main", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:%s", db.DatabaseId, db.BranchId, "test_table")), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME|ALTER_TABLE|deny)",
					args:          []string{"main", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeAlterTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:table:%s", db.DatabaseId, db.BranchId, "test_table")), Actions: []auth.Privilege{auth.DatabasePrivilegeAlterTable}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanAlterTable(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					hasError := false

					_, err = con.GetConnection().Exec("ALTER TABLE test_table ADD COLUMN name_2 TEXT", nil)

					if err != nil {
						hasError = true
					}

					if testCase.expectedError == nil && hasError {
						t.Errorf("Expected no error when altering table")
					}

					if testCase.expectedError != nil && !hasError {
						t.Errorf("Expected error when altering table")
					}
				})
			}
		})

		t.Run("CanCreateIndex", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"idx_test_table_name", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_INDEX|allow)",
					args: []string{"idx_test_table_name", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
				{
					name:          "(*|CREATE_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_INDEX|allow)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_INDEX|allow)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeReindex, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateIndex}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateIndex(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("CREATE INDEX idx_test_table_name ON test_table(name)", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating index")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating index")
					}
				})
			}
		})

		t.Run("CanCreateTable", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_TABLE|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
				{
					name:          "(*|CREATE_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateTable(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating table")
					}
				})
			}
		})

		t.Run("CanCreateTempTable", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_TEMP_TABLE|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
				{
					name:          "(*|CREATE_TEMP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TEMP_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TEMP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTable}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateTempTable(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TEMP TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating table")
					}
				})
			}
		})

		t.Run("CanCreateTempTrigger", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_TEMP_TRIGGER|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTrigger}},
					},
				},
				{
					name:          "(*|CREATE_TEMP_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_TRIGGER|allow)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TEMP_TRIGGER|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempTrigger}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateTempTrigger(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TEMP TRIGGER test_trigger BEFORE INSERT ON test_table BEGIN SELECT RAISE(ABORT, 'Trigger fired!'); END;", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating trigger")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating trigger")
					}
				})
			}
		})

		t.Run("CanCreateTempView", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_TEMP_VIEW|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempView}},
					},
				},
				{
					name:          "(*|CREATE_TEMP_VIEW|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_VIEW|allow)",
					args:          []string{"test_view"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TEMP_VIEW|deny)",
					args:          []string{"test_view", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TEMP_VIEW|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTempView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTempView}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Delete temp view if exists
					_, err = con.GetConnection().Exec("DROP VIEW IF EXISTS test_view", nil)

					if err != nil {
						t.Fatalf("Failed to drop view: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateTempView(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TEMP VIEW test_view AS SELECT * FROM test_table", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating view")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating view")
					}
				})
			}
		})

		t.Run("CanCreateTrigger", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_TRIGGER|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTrigger}},
					},
				},
				{
					name:          "(*|CREATE_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TRIGGER|allow)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_TRIGGER|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTrigger}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateTrigger(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TRIGGER test_trigger BEFORE INSERT ON test_table BEGIN SELECT RAISE(ABORT, 'Trigger fired!'); END;", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating trigger")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating trigger")
					}
				})
			}
		})

		t.Run("CanCreateView", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_VIEW|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateView}},
					},
				},
				{
					name:          "(*|CREATE_VIEW|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_VIEW|allow)",
					args:          []string{"test_view"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_VIEW|deny)",
					args:          []string{"test_view", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_VIEW|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateView}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Delete temp view if exists
					_, err = con.GetConnection().Exec("DROP VIEW IF EXISTS test_view", nil)

					if err != nil {
						t.Fatalf("Failed to drop view: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateView(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE VIEW test_view AS SELECT * FROM test_table", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating view")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating view")
					}
				})
			}
		})

		t.Run("CanCreateVTable", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table", "test_module"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table", "test_module"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateVTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|CREATE_VTABLE|allow)",
					args: []string{"test_table", "test_module"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
				{
					name:          "(*|CREATE_VTABLE|deny)",
					args:          []string{"test_table", "test_module"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateVTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_VTABLE|allow)",
					args:          []string{"test_table", "test_module"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|CREATE_VTABLE|deny)",
					args:          []string{"test_table", "test_module"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateVTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_VTABLE|allow)",
					args:          []string{"test_table", "test_module"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|CREATE_VTABLE|deny)",
					args:          []string{"test_table", "test_module"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateVTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeCreateIndex, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeCreateVTable}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanCreateVTable(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("CREATE VIRTUAL TABLE test_table USING fts5(name)", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating table")
					}
				})
			}
		})

		t.Run("CanDelete", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDelete),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|DELETE|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
				{
					name:          "(*|DELETE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDelete),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DELETE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DELETE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDelete),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DELETE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DELETE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDelete),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanDelete(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("DELETE FROM test_table WHERE id = 1", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when deleting table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when deleting table")
					}
				})
			}
		})

		t.Run("CanDetach", func(t *testing.T) {
			db := test.MockDatabase(app)

			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"",
				[]auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
			)

			// Test the access key permissions directly
			check, err := accessKey.CanDetach(
				db.DatabaseId,
				db.BranchId,
				"test_table",
			)

			if err == nil {
				t.Fatalf("Expected error when checking detach permissions")
			}

			if check {
				t.Fatalf("Should not have detach permissions by default")
			}
		})

		t.Run("CanDropIndex", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"idx_test_table_name", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|DROP_INDEX|allow)",
					args: []string{"idx_test_table_name", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
				{
					name:          "(*|DROP_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_INDEX|allow)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_INDEX|allow)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_INDEX|deny)",
					args:          []string{"idx_test_table_name", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropIndex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropIndex}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					// Create an index for testing
					_, err = con.GetConnection().Exec("CREATE INDEX idx_test_table_name ON test_table(name)", nil)

					if err != nil {
						t.Fatalf("Failed to create index: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanDropIndex(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("DROP INDEX idx_test_table_name", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when dropping index")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when dropping index")
					}
				})
			}
		})

		t.Run("CanDropTable", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|DROP_TABLE|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
				{
					name:          "(*|DROP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_TABLE|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_TABLE|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTable),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTable}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanDropTable(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("DROP TABLE test_table", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when dropping table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when dropping table")
					}
				})
			}
		})

		t.Run("CanDropTrigger", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|DROP_TRIGGER|allow)",
					args: []string{"test_trigger", "test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
				{
					name:          "(*|DROP_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_TRIGGER|allow)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_TRIGGER|allow)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_TRIGGER|deny)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropTrigger),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropTrigger}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					// Create a trigger for testing
					_, err = con.GetConnection().Exec("CREATE TRIGGER test_trigger AFTER INSERT ON test_table BEGIN UPDATE test_table SET name = 'updated' WHERE id = NEW.id; END;", nil)

					if err != nil {
						t.Fatalf("Failed to create trigger: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanDropTrigger(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("DROP TRIGGER test_trigger", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when dropping trigger")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when dropping trigger")
					}
				})
			}
		})

		t.Run("CanDropView", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|DROP_VIEW|allow)",
					args: []string{"test_view"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDropView}},
					},
				},
				{
					name:          "(*|DROP_VIEW|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_VIEW|allow)",
					args:          []string{"test_trigger", "test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|DROP_VIEW|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_VIEW|allow)",
					args:          []string{"test_view"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropView}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|DROP_VIEW|deny)",
					args:          []string{"test_view"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeDropView),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeDropView}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Delete view if exists
					_, err = con.GetConnection().Exec("DROP VIEW IF EXISTS test_view", nil)

					if err != nil {
						t.Fatalf("Failed to drop view: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					// Create a view for testing
					_, err = con.GetConnection().Exec("CREATE VIEW test_view AS SELECT * FROM test_table", nil)

					if err != nil {
						t.Fatalf("Failed to create view: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanDropView(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("DROP VIEW test_view", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when dropping view")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when dropping view")
					}
				})
			}
		})

		t.Run("CanFunction", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"sqlite_version"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"sqlite_version"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeFunction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|FUNCTION|allow)",
					args: []string{"sqlite_version"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(*|FUNCTION|deny)",
					args:          []string{"sqlite_version"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeFunction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|FUNCTION|allow)",
					args:          []string{"sqlite_version"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|FUNCTION|deny)",
					args:          []string{"sqlite_version"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeFunction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|FUNCTION|allow)",
					args:          []string{"sqlite_version"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|FUNCTION|deny)",
					args:          []string{"sqlite_version"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeFunction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeFunction}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanFunction(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("SELECT sqlite_version()", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when using function %q", testCase.args[0])
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when using function %q", testCase.args[0])
					}
				})
			}
		})

		t.Run("CanInsert", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeInsert),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|INSERT|allow)",
					args: []string{"test_table"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
				{
					name:          "(*|INSERT|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeInsert),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|INSERT|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|INSERT|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeInsert),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|INSERT|allow)",
					args:          []string{"test_table"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|INSERT|deny)",
					args:          []string{"test_table"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeInsert),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeInsert}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanInsert(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("INSERT INTO test_table (name) VALUES ('test_insert')", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating table")
					}
				})
			}
		})

		t.Run("CanPragma", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"database_list"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"database_list"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegePragma),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|PRAGMA|allow)",
					args: []string{"database_list"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
				{
					name:          "(*|PRAGMA|deny)",
					args:          []string{"database_list"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegePragma),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|PRAGMA|allow)",
					args:          []string{"database_list"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|PRAGMA|deny)",
					args:          []string{"database_list"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegePragma),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|PRAGMA|allow)",
					args:          []string{"database_list"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|PRAGMA|deny)",
					args:          []string{"database_list"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegePragma),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegePragma}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanPragma(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						"",
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("PRAGMA database_list", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error running PRAGMA command, got: %v", err)
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when running PRAGMA command")
					}
				})
			}
		})

		t.Run("CanRead", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table", "name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table", "name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRead),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|READ|allow)",
					args: []string{"test_table", "name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
				{
					name:          "(*|READ|deny)",
					args:          []string{"test_table", "name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRead),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|READ|allow)",
					args:          []string{"test_table", "name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|READ|deny)",
					args:          []string{"test_table", "name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRead),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|READ|allow)",
					args:          []string{"test_table", "name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|READ|deny)",
					args:          []string{"test_table", "name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRead),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					// Insert a row for testing
					_, err = con.GetConnection().Exec("INSERT INTO test_table (name) VALUES ('test_read')", nil)

					if err != nil {
						t.Fatalf("Failed to insert test row: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanRead(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("SELECT "+testCase.args[1]+" FROM "+testCase.args[0]+" WHERE id = 1", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating table")
					}
				})
			}
		})

		t.Run("CanRecursive", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRecursive),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|RECURSIVE|allow)",
					args: []string{},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
				{
					name:          "(*|RECURSIVE|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRecursive),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|RECURSIVE|allow)",
					args:          []string{},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|RECURSIVE|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRecursive),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|RECURSIVE|allow)",
					args:          []string{},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|RECURSIVE|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeRecursive),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeRecursive}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanRecursive(
						db.DatabaseId,
						db.BranchId,
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Create the table
					_, err = con.GetConnection().Exec("WITH RECURSIVE cte(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM cte WHERE id < 10) SELECT * FROM cte", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when reading recursively")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when reading recursively")
					}
				})
			}
		})

		t.Run("CanReindex", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"idx_test_table_name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"idx_test_table_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeReindex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|REINDEX|allow)",
					args: []string{"idx_test_table_name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
				{
					name:          "(*|REINDEX|deny)",
					args:          []string{"idx_test_table_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeReindex),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|REINDEX|allow)",
					args:          []string{"idx_test_table_name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|REINDEX|deny)",
					args:          []string{"idx_test_table_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeReindex),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|REINDEX|allow)",
					args:          []string{"idx_test_table_name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|REINDEX|deny)",
					args:          []string{"idx_test_table_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeReindex),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeReindex}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("DROP TABLE IF EXISTS test_table", nil)

					if err != nil {
						t.Fatalf("Failed to drop table: %v", err)
					}

					// Create the table
					_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					// CREATE an index for testing
					_, err = con.GetConnection().Exec("CREATE INDEX idx_test_table_name ON test_table(name)", nil)

					if err != nil {
						t.Fatalf("Failed to create index: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanReindex(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("REINDEX idx_test_table_name", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when reindexing index")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when reindexing index")
					}
				})
			}
		})

		t.Run("CanSavepoint", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_operation", "savepoint_name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSavepoint),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|SAVEPOINT|allow)",
					args: []string{"test_operation", "savepoint_name"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
				{
					name:          "(*|SAVEPOINT|deny)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSavepoint),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|SAVEPOINT|allow)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|SAVEPOINT|deny)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSavepoint),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|SAVEPOINT|allow)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|SAVEPOINT|deny)",
					args:          []string{"test_operation", "savepoint_name"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSavepoint),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSavepoint}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanSavepoint(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("SAVEPOINT "+testCase.args[1], nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when creating savepoint")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when creating savepoint")
					}
				})
			}
		})

		t.Run("CanSelect", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSelect),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|SELECT|allow)",
					args: []string{},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
				{
					name:          "(*|SELECT|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSelect),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|SELECT|allow)",
					args:          []string{},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|SELECT|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSelect),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|SELECT|allow)",
					args:          []string{},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|SELECT|deny)",
					args:          []string{},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeSelect),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanSelect(
						db.DatabaseId,
						db.BranchId,
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("SELECT 1", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when selecting data")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when selecting data")
					}
				})
			}
		})

		t.Run("CanTransaction", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_operation"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_operation"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeTransaction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeSelect}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|TRANSACTION|allow)",
					args: []string{"test_operation"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
				{
					name:          "(*|TRANSACTION|deny)",
					args:          []string{"test_operation"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeTransaction),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|TRANSACTION|allow)",
					args:          []string{"test_operation"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|TRANSACTION|deny)",
					args:          []string{"test_operation"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeTransaction),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|TRANSACTION|allow)",
					args:          []string{"test_operation"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|TRANSACTION|deny)",
					args:          []string{"test_operation"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeTransaction),
					statements: []auth.AccessKeyStatement{
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeTransaction}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanTransaction(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					_, err = con.GetConnection().Exec("BEGIN", nil)

					if err == nil {
						_, err = con.GetConnection().Exec("COMMIT", nil)
					}

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when starting transaction")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when starting transaction")
					}
				})
			}
		})

		t.Run("CanUpdate", func(t *testing.T) {
			db := test.MockDatabase(app)

			con, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

			if err != nil {
				t.Fatalf("Failed to get connection for test: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

			testCases := []struct {
				name          string
				args          []string
				expectedError error
				statements    []auth.AccessKeyStatement
			}{
				{
					name: "(*|*|allow)",
					args: []string{"test_table", "test_column"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name:          "(*|*|deny)",
					args:          []string{"test_table", "test_column"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeUpdate),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
				},
				{
					name: "(*|UPDATE|allow)",
					args: []string{"test_table", "test_column"},
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
				{
					name:          "(*|UPDATE|deny)",
					args:          []string{"test_table", "test_column"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeUpdate),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectDeny, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|UPDATE|allow)",
					args:          []string{"test_table", "test_column"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
				{
					name:          "(database:DATABASE_ID:*|UPDATE|deny)",
					args:          []string{"test_table", "test_column"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeUpdate),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:*", db.DatabaseId)), Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|UPDATE|allow)",
					args:          []string{"test_table", "test_column"},
					expectedError: nil,
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: auth.AccessKeyEffectAllow, Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
				{
					name:          "(database:DATABASE_ID:branch:BRANCH_ID:*|UPDATE|deny)",
					args:          []string{"test_table", "test_column"},
					expectedError: auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeUpdate),
					statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
						{Effect: "DENY", Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:%s:*", db.DatabaseId, db.BranchId)), Actions: []auth.Privilege{auth.DatabasePrivilegeUpdate}},
					},
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					con.WithAccessKey(nil)

					// Delete table if exists
					_, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

					if err != nil {
						t.Fatalf("Failed to create table: %v", err)
					}

					accessKey := auth.NewAccessKey(
						app.Auth.AccessKeyManager,
						"accessKeyId",
						"accessKeySecret",
						"",
						testCase.statements,
					)

					// Test the access key permissions directly
					check, err := accessKey.CanUpdate(
						db.DatabaseId,
						db.BranchId,
						testCase.args[0],
						testCase.args[1],
					)

					if err != testCase.expectedError {
						t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
					}

					if testCase.expectedError != nil && check != false {
						t.Errorf("Expected check to be %v, got %v", true, check)
					}

					con.WithAccessKey(accessKey)

					// Update the table
					_, err = con.GetConnection().Exec("UPDATE test_table SET name = 'test_update' WHERE name = 'test_insert'", nil)

					if testCase.expectedError == nil && err != nil {
						t.Errorf("Expected no error when updating table")
					}

					if testCase.expectedError != nil && err == nil {
						t.Errorf("Expected error when updating table")
					}
				})
			}
		})
	})
}
