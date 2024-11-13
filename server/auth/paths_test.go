package auth_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/auth"
	"testing"
)

func TestGetDatabaseKeyPath(t *testing.T) {

	test.RunWithApp(t, func(app *server.App) {
		path := fmt.Sprintf("%s%s/%s", auth.Path(app.Config.Signature), "database_keys", "test")

		if auth.GetDatabaseKeyPath(app.Config.Signature, "test") != path {
			t.Error("Expected GetDatabaseKeyPath to return the string 'test'")
		}
	})
}
