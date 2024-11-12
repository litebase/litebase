package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"testing"
	"time"
)

func TestNewNodeWALReplicator(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		replicator := cluster.NewNodeWALReplicator(app.Cluster.Node())

		if replicator == nil {
			t.Error("expected replicator to not be nil")
		}
	})
}

func TestNodeWALReplicatorTruncate(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		test.NewTestServer(t)
		test.NewTestServer(t)

		db := test.MockDatabase(testServer1.App)

		replicator := cluster.NewNodeWALReplicator(testServer1.App.Cluster.Node())

		err := replicator.Truncate(
			db.DatabaseId,
			db.BranchId,
			0,
			0,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected error to be nil, got %v", err)
		}
	})
}

func TestNodeWALReplicatorWriteAt(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		test.NewTestServer(t)
		test.NewTestServer(t)

		db := test.MockDatabase(testServer1.App)

		replicator := cluster.NewNodeWALReplicator(testServer1.App.Cluster.Node())

		err := replicator.WriteAt(
			db.DatabaseId,
			db.BranchId,
			[]byte("hello"),
			0,
			1,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected error to be nil, got %v", err)
		}
	})
}
