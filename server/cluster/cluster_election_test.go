package cluster_test

import (
	"sync"
	"testing"
	"time"

	"slices"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster"
)

func TestNewClusterElection(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		election := cluster.NewClusterElection(
			server.Cluster.Node(),
		)

		if election == nil {
			t.Fatal("Expected NewClusterElection to return a non-nil value")
		}
	})
}

func TestClusterElection_Expired(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		election := cluster.NewClusterElection(
			server.Cluster.Node(),
		)

		election.EndsAt = time.Now().Add(-time.Second) // Set the election to expired

		if !election.Expired() {
			t.Fatal("Expected election to be expired")
		}
	})
}

func TestClusterElection_Running(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		election := cluster.NewClusterElection(
			server.Cluster.Node(),
		)

		if !election.Running() {
			t.Fatal("Expected election to be running")
		}

		election.StoppedAt = time.Now().Add(-10 * time.Second)

		if election.Running() {
			t.Fatal("Expected election to not be running")
		}
	})
}

func TestClusterElection_Stop(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		election := cluster.NewClusterElection(
			server.Cluster.Node(),
		)

		if !election.Running() {
			t.Fatal("Expected election to be running before stopping")
		}

		election.Stop()

		if election.Running() {
			t.Fatal("Expected election to not be running after stopping")
		}
	})
}

func TestClusterElection_Stopped(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		election := cluster.NewClusterElection(
			server.Cluster.Node(),
		)

		if election.Stopped() {
			t.Fatal("Expected election to not be stopped before stopping")
		}

		election.Stop()

		if !election.Stopped() {
			t.Fatal("Expected election to be stopped after stopping")
		}
	})
}

func TestClusterElectionRunWithMultipleNodesSynchronous(t *testing.T) {
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
		{nodeCount: 9},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make([]*test.TestServer, tc.nodeCount)

				for i := range tc.nodeCount {
					servers[i] = test.NewTestServer(t)
					<-servers[i].Started
				}

				timeout := time.After(3 * time.Second)

			outerLoop:
				for {
					select {
					case <-timeout:
						t.Fatalf("Election timed out after 3 seconds")
					default:
						var electedCount int
						var primaryAddress string
						var allObservedPrimary bool = true

						for _, server := range servers {
							if server.App.Cluster.Node().Membership == cluster.ClusterMembershipPrimary {
								electedCount++
								primaryAddress = server.App.Cluster.Node().PrimaryAddress()
							}
						}

						if electedCount == 1 && primaryAddress != "" {
							for _, server := range servers {
								if server.App.Cluster.Node().Membership != cluster.ClusterMembershipPrimary {
									if server.App.Cluster.Node().PrimaryAddress() != primaryAddress {
										allObservedPrimary = false
										break
									}
								}
							}

							if allObservedPrimary {
								break outerLoop
							}
						}

						time.Sleep(10 * time.Millisecond) // Sleep to avoid busy waiting
					}
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
		{nodeCount: 9},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			test.Run(t, func() {
				servers := make(map[int]*test.TestServer, tc.nodeCount)
				serversMutex := sync.Mutex{}

				// Start the first server to initialize the cluster
				server := test.NewTestServer(t)
				<-server.Started
				server.Shutdown()

				wg := sync.WaitGroup{}
				wg.Add(tc.nodeCount)

				for i := range tc.nodeCount {
					go func(i int) {
						defer wg.Done()

						serversMutex.Lock()
						server := test.NewTestServer(t)
						servers[i] = server
						serversMutex.Unlock()

						<-server.Started
					}(i)
				}

				wg.Wait()

				timeout := time.After(3 * time.Second)

			outerLoop:
				for {
					select {
					case <-timeout:
						t.Fatalf("Election timed out after 3 seconds")
					default:
						var electedCount int
						var primaryAddress string
						var allObservedPrimary bool = true

						for _, server := range servers {
							if server.App.Cluster.Node().IsPrimary() {
								electedCount++
								primaryAddress = server.App.Cluster.Node().PrimaryAddress()

								if primaryAddress == "" {
									t.Fatal("Primary address is empty")
								}
							}
						}

						if electedCount == 1 && primaryAddress != "" {
							for _, server := range servers {
								nodePrimaryAddress := server.App.Cluster.Node().PrimaryAddress()
								if nodePrimaryAddress != primaryAddress {
									allObservedPrimary = false
									break
								}
							}

							if allObservedPrimary {
								break outerLoop
							}
						}

						time.Sleep(10 * time.Millisecond) // Sleep to avoid busy waiting
					}
				}

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}

func TestClusterElectionRunWithMultipleNodesAsyncWithStoppingServers(t *testing.T) {
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
				serversMutex := sync.Mutex{}

				// Start the first server to initialize the cluster
				server := test.NewTestServer(t)
				<-server.Started
				server.Shutdown()

				wg := sync.WaitGroup{}
				wg.Add(tc.nodeCount)

				for i := range tc.nodeCount {
					go func(i int) {
						defer wg.Done()

						serversMutex.Lock()
						server := test.NewTestServer(t)
						servers[i] = server
						serversMutex.Unlock()

						<-server.Started
					}(i)
				}

				wg.Wait()

				// Continue looping until we have 1 server left, removing the
				// primary server each time.
				for {
					timeout := time.After(10 * time.Second)
					var electedIndex int = -1

				outerLoop:
					for {
						select {
						case <-timeout:
							t.Fatalf("Election timed out after 5 seconds")
						default:
							var electedCount int
							var primaryAddress string
							var allObservedPrimary bool = true

							for i, server := range servers {
								if server.App.Cluster.Node().IsPrimary() {
									electedCount++
									electedIndex = i
									primaryAddress = server.App.Cluster.Node().PrimaryAddress()

									if primaryAddress == "" {
										t.Fatal("Primary address is empty")
									}
								}
							}

							if electedCount == 1 && primaryAddress != "" {
								for _, server := range servers {
									nodePrimaryAddress := server.App.Cluster.Node().PrimaryAddress()
									if nodePrimaryAddress != primaryAddress {
										allObservedPrimary = false
										break
									}
								}

								if allObservedPrimary {
									break outerLoop
								}
							}

							time.Sleep(10 * time.Millisecond) // Sleep to avoid busy waiting
						}
					}

					if electedIndex >= 0 {
						servers[electedIndex].Shutdown()
						servers = slices.Delete(servers, electedIndex, electedIndex+1)
					}

					if len(servers) == 1 {
						break
					}
				}

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}
