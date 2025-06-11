package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/server/auth"
)

type mockPrivilege string

func TestNewDatabasePrivilegeError(t *testing.T) {
	priv := mockPrivilege("ADMIN")
	err := auth.NewDatabasePrivilegeError(auth.Privilege(priv))

	expected := fmt.Sprintf("'Authorization Denied: The %s privilege is required to perform this query.", priv)

	if err.Error() != expected {
		t.Fatalf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestDatabasePrivilegeError_Error(t *testing.T) {
	err := auth.NewDatabasePrivilegeError(auth.Privilege("TEST"))
	expected := "'Authorization Denied: The TEST privilege is required to perform this query."
	if err.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, err.Error())
	}
}
