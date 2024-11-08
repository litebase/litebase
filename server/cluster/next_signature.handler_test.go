package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"testing"
)

func TestNextSignatureHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cluster.NextSignatureHandler(app.Config, "data")

		if app.Config.SignatureNext != "data" {
			t.Fatalf("Signature not set correctly: %s", app.Config.SignatureNext)
		}
	})
}
