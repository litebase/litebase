package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
)

func TestEventStoreController(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		var eventReceived bool

		server2.App.Cluster.Subscribe("foo", func(message *cluster.EventMessage) {
			if message.Key != "foo" {
				t.Errorf("Expected key 'foo', got %s", message.Key)
			}

			if message.Value != "bar" {
				t.Errorf("Expected event 'bar', got %s", message.Value)
			}

			eventReceived = true
		})

		otherNodes := server1.App.Cluster.OtherNodes()

		if len(otherNodes) == 0 {
			t.Fatalf("Expected at least one other node")
		}

		var nodeIdentifier *cluster.NodeIdentifier

		for _, node := range otherNodes {
			if node.Address == server2.Address {
				nodeIdentifier = node
				break
			}
		}

		err := server1.App.Cluster.SendEvent(nodeIdentifier, "foo", "bar")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !eventReceived {
			t.Fatalf("Expected event to be received")
		}
	})
}
