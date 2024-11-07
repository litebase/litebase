package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"math/rand/v2"
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

func TestClusterElectionAddCandidate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		clusterElection.AddCandidate("test", clusterElection.Seed+1)

		if len(clusterElection.Candidates) != 2 {
			t.Fatalf("Expected 2 candidates, got %d", len(clusterElection.Candidates))
		}

		if clusterElection.Nominee != "test" {
			t.Errorf("Expected nominee to be 'test', got %s", clusterElection.Nominee)
		}
	})
}

func TestClusterElectionContext(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		if clusterElection.Context() == nil {
			t.Fatalf("Expected context to not be nil")
		}
	})
}

func TestClusterElectionRun(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		elected, err := clusterElection.Run()

		if err != nil {
			t.Fatalf("Expected error, got nil")
		}

		if !elected {
			t.Fatalf("Expected elected to be true")
		}
	})
}

func TestClusterElectionRunWithMultipleNodes(t *testing.T) {
	testCases := []struct {
		nodeCount int
	}{
		{nodeCount: 1},
		{nodeCount: 3},
		{nodeCount: 2},
		{nodeCount: 4},
		{nodeCount: 5},
		{nodeCount: 6},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make([]*test.TestServer, tc.nodeCount)

				for i := 0; i < tc.nodeCount; i++ {
					servers[i] = test.NewUnstartedTestServer(t)
				}

				var electedCount int

				for i := 0; i < tc.nodeCount; i++ {
					servers[i].App.Cluster.Node().JoinCluster()
				}

				for i := 0; i < tc.nodeCount; i++ {
					clusterElection := servers[i].App.Cluster.Node().Election()

					for j := 0; j < tc.nodeCount; j++ {
						if i == j {
							continue
						}

						seed := rand.Int64()
						clusterElection.AddCandidate(servers[j].App.Cluster.Node().Address(), seed)
					}

					elected, err := clusterElection.Run()

					if err != nil {
						t.Errorf("Expected error, got nil")
					}

					if elected {
						electedCount++
					}
				}

				if electedCount != 1 {
					t.Fatalf("Expected 1 elected, got %d", electedCount)
				}
			})
		})
	}
}

func TestClusterElectionStop(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		clusterElection.Stop()

		if clusterElection.Context().Err() == nil {
			t.Fatalf("Expected context error, got nil")
		}
	})
}
