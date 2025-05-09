package cluster_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster"
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
		{nodeCount: 7},
		{nodeCount: 8},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make([]*test.TestServer, tc.nodeCount)

				for i := range tc.nodeCount {
					servers[i] = test.NewUnstartedTestServer(t)
				}

				var electedCount int

				for _, server := range servers {
					server.App.Cluster.Node().JoinCluster()
				}

				for _, server := range servers {
					clusterElection := server.App.Cluster.Node().Election()

					elected, err := clusterElection.Run()

					if err != nil {
						t.Fatalf("run election error: %v", err)
					}

					if elected {
						electedCount++
					}
				}

				if electedCount != 1 {
					t.Fatalf("Expected 1 elected, got %d", electedCount)
				}

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}

func TestClusterElectionRunWithMultipleNodesAsync(t *testing.T) {
	testCases := []struct {
		nodeCount int
	}{
		{nodeCount: 1},
		{nodeCount: 2},
		{nodeCount: 3},
		{nodeCount: 4},
		{nodeCount: 5},
		{nodeCount: 6},
		{nodeCount: 7},
		{nodeCount: 8},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make([]*test.TestServer, tc.nodeCount)

				for i := range tc.nodeCount {
					servers[i] = test.NewUnstartedTestServer(t)
				}

				var electedCount int

				for _, server := range servers {
					server.App.Cluster.Node().JoinCluster()
				}

				wg := sync.WaitGroup{}
				wg.Add(tc.nodeCount)

				var electionErrors []error

				for i, server := range servers {
					go func(server *test.TestServer, i int) {
						defer wg.Done()

						clusterElection := server.App.Cluster.Node().Election()

						elected, err := clusterElection.Run()

						if err != nil {
							electionErrors = append(electionErrors, err)

							return
						}

						if elected {
							electedCount++
						}
					}(server, i)
				}

				wg.Wait()

				if len(electionErrors) > 0 {
					t.Fatalf("run election error: %v", electionErrors)
				}

				if electedCount != 1 {
					t.Fatalf("Expected 1 elected, got %d", electedCount)
				}

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}

func TestClusterElectionRunWithMultipleNodesAsyncWithFailure(t *testing.T) {
	testCases := []struct {
		nodeCount int
	}{
		{nodeCount: 2},
		{nodeCount: 3},
		{nodeCount: 4},
		{nodeCount: 5},
		{nodeCount: 6},
		{nodeCount: 7},
		{nodeCount: 8},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make([]*test.TestServer, tc.nodeCount)

				for i := range tc.nodeCount {
					servers[i] = test.NewUnstartedTestServer(t)
				}

				var electedCount int

				for _, server := range servers {
					server.App.Cluster.Node().JoinCluster()
				}

				for len(servers) > 1 {
					wg := sync.WaitGroup{}
					wg.Add(len(servers))

					var electionErrors []error
					var electedIndex int

					for i, server := range servers {
						go func(server *test.TestServer, i int) {
							defer wg.Done()

							clusterElection := server.App.Cluster.Node().Election()

							elected, err := clusterElection.Run()

							if err != nil {
								electionErrors = append(electionErrors, err)

								return
							}

							if elected {
								electedCount++
								electedIndex = i
							}
						}(server, i)
					}

					wg.Wait()

					if len(electionErrors) > 0 {
						t.Fatalf("run election error: %v", electionErrors)
					}

					if electedCount != 1 {
						t.Fatalf("Expected 1 elected, got %d", electedCount)
						break
					}

					servers[electedIndex].Shutdown()
					servers = slices.Delete(servers, electedIndex, electedIndex+1)
					electedCount = 0
				}

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}

func TestClusterElectionStop(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		clusterElection := cluster.NewClusterElection(app.Cluster.Node(), time.Now())

		clusterElection.Run()
		clusterElection.Stop()

		ctx := clusterElection.Context()

		if ctx == nil {
			t.Fatalf("Expected context to not be nil")
		}

		if ctx.Err() == nil {
			t.Fatalf("Expected context error, got nil")
		}
	})
}
