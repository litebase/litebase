package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestAccessKeyCanAccessDatabase(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		accessKey := auth.NewAccessKey(
			app.Auth.AccessKeyManager,
			"accessKeyId",
			"accessKeySecret",
			[]*auth.AccessKeyPermission{
				{
					Resource: "*",
					Actions:  []string{"*"},
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
}

func TestAccessKeyCanAlterTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		testCases := []struct {
			args           []string
			expectedError  error
			expectedResult bool
			permissions    []*auth.AccessKeyPermission
		}{
			{
				args:           []string{""},
				expectedError:  auth.NewDatabasePrivilegeError("ALTER_TABLE"),
				expectedResult: false,
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"*"},
					},
				},
			},
			{
				args:           []string{"sqlite_master"},
				expectedError:  auth.NewDatabasePrivilegeError("INDEX"),
				expectedResult: false,
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"table:ALTER"},
					},
				},
			},
			{
				args:           []string{"sqlite_master"},
				expectedError:  nil,
				expectedResult: true,
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"table:ALTER", "table:INDEX"},
					},
				},
			},
			{
				args:           []string{"main", "test"},
				expectedError:  nil,
				expectedResult: true,
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"table:ALTER"},
					},
				},
			},
		}

		for _, testCase := range testCases {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				testCase.permissions,
			)

			check, err := accessKey.CanAlterTable(
				db.DatabaseId,
				db.BranchId,
				testCase.args[0],
				testCase.args[1],
			)

			if err != testCase.expectedError {
				t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
			}

			if check != testCase.expectedResult {
				t.Errorf("Expected check to be %v, got %v", testCase.expectedResult, check)
			}
		}
	})
}

func TestAccessKeyCanCreate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		testCases := []struct {
			args           []string
			expectedError  error
			expectedResult bool
			permissions    []*auth.AccessKeyPermission
		}{
			{
				args:           []string{},
				expectedError:  auth.NewDatabasePrivilegeError(auth.DatabasePrivilegeCreateTable),
				expectedResult: false,
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"*"},
					},
				},
			},
			{},
		}

		for _, testCase := range testCases {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				testCase.permissions,
			)

			check, err := accessKey.CanCreateTable(
				db.DatabaseId,
				db.BranchId,
				"test",
			)

			if err != testCase.expectedError {
				t.Errorf("Expected error to be %v, got %v", testCase.expectedError, err)
			}

			if check != testCase.expectedResult {
				t.Errorf("Expected check to be %v, got %v", testCase.expectedResult, check)
			}
		}
	})
}
