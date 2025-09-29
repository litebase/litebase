package http_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
)

func TestEventStoreController(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		var eventReceived int32

		server2.App.Cluster.Subscribe("foo", func(message *cluster.EventMessage) {
			if message.Key != "foo" {
				t.Errorf("Expected key 'foo', got %s", message.Key)
			}

			if message.Value != "bar" {
				t.Errorf("Expected event 'bar', got %s", message.Value)
			}

			atomic.StoreInt32(&eventReceived, 1)
		})

		otherNodes := server1.App.Cluster.OtherNodes()
		if len(otherNodes) == 0 {
			t.Fatalf("Expected at least one other node")
		}

		var nodeIdentifier *cluster.NodeIdentifier

		// Get server2's node address (private address)
		server2NodeAddress, _ := server2.App.Cluster.Node().Address()

		for _, node := range otherNodes {
			if node.Address == server2NodeAddress {
				nodeIdentifier = node
				break
			}
		}

		if nodeIdentifier == nil {
			t.Fatalf("Could not find matching node identifier for server2 node address %s", server2NodeAddress)
		}

		err := server1.App.Cluster.SendEvent(nodeIdentifier, "foo", "bar")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Wait a bit for the event to be processed
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&eventReceived) == 1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if atomic.LoadInt32(&eventReceived) != 1 {
			t.Fatalf("Expected event to be received")
		}
	})
}
