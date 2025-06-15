package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestGetDatabaseKeysPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := fmt.Sprintf("%s%s", auth.Path(app.Config.Signature), "DATABASE_KEYS")

		if auth.GetDatabaseKeysPath(app.Config.Signature) != path {
			t.Errorf("Expected GetDatabaseKeysPath to return %s, got %s", path, auth.GetDatabaseKeysPath(app.Config.Signature))
		}
	})
}
