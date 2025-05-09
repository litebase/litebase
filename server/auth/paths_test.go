package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestGetDatabaseKeyPath(t *testing.T) {

	test.RunWithApp(t, func(app *server.App) {
		path := fmt.Sprintf("%s%s/%s", auth.Path(app.Config.Signature), "database_keys", "test")

		if auth.GetDatabaseKeyPath(app.Config.Signature, "test") != path {
			t.Error("Expected GetDatabaseKeyPath to return the string 'test'")
		}
	})
}
