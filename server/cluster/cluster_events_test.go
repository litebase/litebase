package cluster_test

import (
	"litebase/internal/test"
	"litebase/server/cluster"
	"testing"
	"time"
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

func TestReceiveEvent(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

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
}

func TestSubscribe(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

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
}
