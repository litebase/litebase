package auth_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyStatement(t *testing.T) {
	tc := []struct {
		resource string
		actions  []auth.Privilege
		valid    bool
	}{
		{"*", []auth.Privilege{"*"}, true},
		{"*", []auth.Privilege{"access-key:create"}, true},
		{"access-key:*", []auth.Privilege{auth.AccessKeyPrivilegeCreate}, true},
		{"access-key:*", []auth.Privilege{auth.DatabasePrivilegeAlterTable}, false},
		{"database:*", []auth.Privilege{auth.AccessKeyPrivilegeCreate}, false},
	}

	for _, testCase := range tc {
		aks := &auth.AccessKeyStatement{
			Effect:   auth.AccessKeyEffectAllow,
			Resource: auth.AccessKeyResource(testCase.resource),
			Actions:  testCase.actions,
		}

		if aks.IsValid() != testCase.valid {
			t.Error("Expected access key statement to be valid")
		}
	}
}
