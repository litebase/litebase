package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"log"
	"testing"
)

func TestBroadcast(t *testing.T) {
	cluster.SetAddressProvider(func() string {
		return "localhost"
	})
	// Create a new node instance
	test.Run(t, func(app *server.App) {
		ts1 := test.NewTestServer(t)
		ts2 := test.NewTestServer(t)
		log.Println(ts1.Server.URL)
		log.Println(ts2.Server.URL)
	})
}
