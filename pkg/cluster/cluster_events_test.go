package cluster_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
)

func TestEvents(t *testing.T) {
	test.Run(t, func() {
		t.Run("TestBroadcast", func(t *testing.T) {
			cluster.SetAddressProvider(func() string {
				return "127.0.0.1"
			})

			// Create a new node instance
			server1 := test.NewTestServer(t)
			server2 := test.NewTestServer(t)

			defer server1.Shutdown()
			defer server2.Shutdown()

			var receivedMessage *cluster.EventMessage

			server2.App.Cluster.Subscribe("test", func(message *cluster.EventMessage) {
				receivedMessage = message
			})

			err := server1.App.Cluster.Broadcast("test", "test")

			if err != nil {
				t.Error(err)
			}

			if receivedMessage == nil {
				t.Fatal("Message not received")
			}

			if receivedMessage.Key != "test" {
				t.Error("Invalid message key")
			}

			if receivedMessage.Value != "test" {
				t.Error("Invalid message value")
			}
		})

		t.Run("TestReceiveEvent", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			var receivedMessage *cluster.EventMessage

			server.App.Cluster.Subscribe("test", func(message *cluster.EventMessage) {
				receivedMessage = message
			})

			server.App.Cluster.ReceiveEvent(&cluster.EventMessage{
				Key:   "test",
				Value: "test",
			})

			time.Sleep(10 * time.Millisecond)

			if receivedMessage == nil {
				t.Error("Message not received")
			}

			if receivedMessage.Key != "test" {
				t.Error("Invalid message key")
			}

			if receivedMessage.Value != "test" {
				t.Error("Invalid message value")
			}
		})

		t.Run("TestSubscribe", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			var receivedMessage *cluster.EventMessage

			server.App.Cluster.Subscribe("test", func(message *cluster.EventMessage) {
				receivedMessage = message
			})

			server.App.Cluster.ReceiveEvent(&cluster.EventMessage{
				Key:   "test",
				Value: "test",
			})

			time.Sleep(10 * time.Millisecond)

			if receivedMessage == nil {
				t.Error("Message not received")
			}

			if receivedMessage.Key != "test" {
				t.Error("Invalid message key")
			}

			if receivedMessage.Value != "test" {
				t.Error("Invalid message value")
			}
		})
	})
}
