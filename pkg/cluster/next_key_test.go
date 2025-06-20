package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestNextKeyHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cluster.NextKeyHandler(app.Config, "data")

		if app.Config.EncryptionKeyNext != "data" {
			t.Fatalf("Key not set correctly: %s", app.Config.EncryptionKeyNext)
		}
	})
}
