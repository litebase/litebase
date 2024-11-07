package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"testing"
	"time"
)

func TestNewClusterElection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		if clusterElection == nil {
			t.Fatalf("Expected clusterElection to not be nil")
		}
	})
}
