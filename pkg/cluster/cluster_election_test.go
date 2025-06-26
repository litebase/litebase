package cluster_test

import (
	"log"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestClusterElection(t *testing.T) {
	test.RunWithApp(t, func(server *server.App) {
		t.Run("NewClusterElection", func(t *testing.T) {
			election := cluster.NewClusterElection(
				server.Cluster.Node(),
			)

			if election == nil {
				t.Fatal("Expected NewClusterElection to return a non-nil value")
			}
		})

		t.Run("Expired", func(t *testing.T) {
			election := cluster.NewClusterElection(
				server.Cluster.Node(),
			)

			election.EndsAt = time.Now().UTC().Add(-time.Second) // Set the election to expired

			if !election.Expired() {
				t.Fatal("Expected election to be expired")
			}
		})

		t.Run("Running", func(t *testing.T) {
			election := cluster.NewClusterElection(
				server.Cluster.Node(),
			)

			if !election.Running() {
				t.Fatal("Expected election to be running")
			}

			election.StoppedAt = time.Now().UTC().Add(-10 * time.Second)

			if election.Running() {
				t.Fatal("Expected election to not be running")
			}
		})

		t.Run("Stop", func(t *testing.T) {
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

		t.Run("Stopped", func(t *testing.T) {
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

		t.Run("RunWithMultipleNodesSynchronous", func(t *testing.T) {
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
					servers := make([]*test.TestServer, tc.nodeCount)

					test.RunWithTearDown(t, func() {
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

								for _, s := range servers {
									if s.App.Cluster.Node().Membership == cluster.ClusterMembershipPrimary {
										electedCount++
										primaryAddress = s.App.Cluster.Node().PrimaryAddress()
									}
								}

								if electedCount == 1 && primaryAddress != "" {
									for _, s := range servers {
										if s.App.Cluster.Node().Membership != cluster.ClusterMembershipPrimary {
											if s.App.Cluster.Node().PrimaryAddress() != primaryAddress {
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
					}, func() {
						for _, s := range servers {
							s.Shutdown()
						}
					})
				})
			}
		})

		t.Run("RunWithMultipleNodesAsync", func(t *testing.T) {
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
					servers := make(map[int]*test.TestServer, tc.nodeCount)

					test.RunWithTearDown(t, func() {
						serversMutex := sync.Mutex{}

						// Start the first server to initialize the cluster
						s := test.NewTestServer(t)
						<-s.Started
						s.Shutdown()

						wg := sync.WaitGroup{}
						wg.Add(tc.nodeCount)

						for i := range tc.nodeCount {
							go func(i int) {
								defer wg.Done()

								serversMutex.Lock()
								s := test.NewTestServer(t)
								servers[i] = s
								serversMutex.Unlock()

								<-s.Started
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

								for _, s := range servers {
									if s.App.Cluster.Node().IsPrimary() {
										electedCount++
										primaryAddress = s.App.Cluster.Node().PrimaryAddress()

										if primaryAddress == "" {
											t.Fatal("Primary address is empty")
										}
									}
								}

								if electedCount == 1 && primaryAddress != "" {
									for _, s := range servers {
										nodePrimaryAddress := s.App.Cluster.Node().PrimaryAddress()
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
					}, func() {
						for _, s := range servers {
							s.Shutdown()
						}
					})
				})
			}
		})

		t.Run("RunWithMultipleNodesAsyncWithStoppingServers", func(t *testing.T) {
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
					servers := make([]*test.TestServer, tc.nodeCount)

					test.RunWithTearDown(t, func() {
						serversMutex := sync.Mutex{}

						// Start the first server to initialize the cluster
						s := test.NewTestServer(t)
						<-s.Started
						s.Shutdown()

						wg := sync.WaitGroup{}
						wg.Add(tc.nodeCount)

						for i := range tc.nodeCount {
							go func(i int) {
								defer wg.Done()

								serversMutex.Lock()
								s := test.NewTestServer(t)
								servers[i] = s
								serversMutex.Unlock()

								<-s.Started
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
									t.Fatalf("Election timed out after 10 seconds")
								default:
									var electedCount int
									var primaryAddress string
									var allObservedPrimary bool = true

									for i, s := range servers {
										if s.App.Cluster.Node().IsPrimary() {
											electedCount++
											electedIndex = i
											primaryAddress = s.App.Cluster.Node().PrimaryAddress()

											if primaryAddress == "" {
												t.Fatal("Primary address is empty")
											}
										}
									}

									if electedCount == 1 && primaryAddress != "" {
										for _, s := range servers {
											nodePrimaryAddress := s.App.Cluster.Node().PrimaryAddress()
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
					}, func() {
						for _, s := range servers {
							s.Shutdown()
						}
					})
				})
			}
		})
	})
}

func TestClusterElection_WithStepProcessing(t *testing.T) {
	t.Run("WillRunAfterCrashAndNewNodeStarted", func(t *testing.T) {
		// Speed up the lease duration for testing purposes
		defaultLeaseDuration := cluster.LeaseDuration
		defer func() { cluster.LeaseDuration = defaultLeaseDuration }()
		cluster.LeaseDuration = 1 * time.Second

		defaultNodeStoreAddressInterval := cluster.NodeStoreAddressInterval
		defer func() { cluster.NodeStoreAddressInterval = defaultNodeStoreAddressInterval }()
		cluster.NodeStoreAddressInterval = 1 * time.Second

		test.WithSteps(t, func(sp *test.StepProcessor) {
			sp.Run("SERVER_1", func(s *test.StepProcess) {
				test.RunWithoutCleanup(t, func(app *server.App) {
					s.Step("SERVER_1_READY")
					os.Exit(1) // Simulate a crash
				})
			}).ShouldExitWith(1)

			sp.Run("SERVER_2", func(s *test.StepProcess) {
				s.WaitForStep("SERVER_1_READY")

				test.RunWithoutCleanup(t, func(app *server.App) {
					timeout := time.After(15 * time.Second)

				waitForPrimary:
					for {
						select {
						case <-timeout:
							t.Fatal("Timed out waiting for node to become primary")
						default:
							if app.Cluster.Node().IsPrimary() {
								break waitForPrimary
							}
							time.Sleep(100 * time.Millisecond)
						}
					}

					if !app.Cluster.Node().IsPrimary() {
						t.Fatal("Server 2 is not primary after 15 seconds")
					}
				})
			})
		})
	})

	t.Run("WillRunAfterCrashAndAnotherNodeRunning", func(t *testing.T) {
		// Speed up the lease duration for testing purposes
		defaultLeaseDuration := cluster.LeaseDuration
		defer func() { cluster.LeaseDuration = defaultLeaseDuration }()
		cluster.LeaseDuration = 1 * time.Second

		defaultNodeStoreAddressInterval := cluster.NodeStoreAddressInterval
		defer func() { cluster.NodeStoreAddressInterval = defaultNodeStoreAddressInterval }()
		cluster.NodeStoreAddressInterval = 1 * time.Second

		test.WithSteps(t, func(sp *test.StepProcessor) {
			sp.Run("PRIMARY", func(s *test.StepProcess) {
				test.RunWithoutCleanup(t, func(app *server.App) {
					time.Sleep(1 * time.Second)
					s.Step("PRIMARY_READY")
					os.Exit(1) // Simulate a crash
				})
			}).ShouldExitWith(1)

			sp.Run("REPLICA", func(s *test.StepProcess) {
				s.WaitForStep("PRIMARY_READY")

				test.RunWithoutCleanup(t, func(app *server.App) {
					timeout := time.After(15 * time.Second)

				waitForPrimary:
					for {
						select {
						case <-timeout:
							t.Fatal("Timed out waiting for node to become primary")
						default:
							log.Println("Checking if node is primary...")
							if app.Cluster.Node().IsPrimary() {
								break waitForPrimary
							}

							time.Sleep(10 * time.Millisecond)
						}
					}
				})
			})
		})
	})
}
