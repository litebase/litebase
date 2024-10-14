package cluster_test

import (
	"litebase/internal/test"
	"litebase/server/cluster"
	"testing"
)

func TestBroadcast(t *testing.T) {
	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Create a new node instance
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)

		var receivedMessage *cluster.EventMessage

		server2.App.Cluster.Subscribe("test", func(message *cluster.EventMessage) {
			receivedMessage = message
		})

		err := server1.App.Cluster.Broadcast("test", "test")

		if err != nil {
			t.Error(err)
		}

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
}
