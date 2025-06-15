package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestNextSignatureHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cluster.NextSignatureHandler(app.Config, "data")

		if app.Config.SignatureNext != "data" {
			t.Fatalf("Signature not set correctly: %s", app.Config.SignatureNext)
		}
	})
}
