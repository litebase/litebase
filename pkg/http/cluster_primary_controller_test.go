package http_test

import (
	"bytes"
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster/messages"
	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestClusterPrimaryController(t *testing.T) {
	test.Run(t, func() {
		// Create a primary server
		primaryServer := test.NewTestServer(t)
		defer primaryServer.Shutdown()

		// Create a replica server
		replicaServer := test.NewTestServer(t)
		defer replicaServer.Shutdown()

		// Ensure the primary server is set as primary
		if !primaryServer.App.Cluster.Node().IsPrimary() {
			t.Fatalf("primary server should be primary")
		}

		// Ensure the replica server is set as replica
		if !replicaServer.App.Cluster.Node().IsReplica() {
			t.Fatalf("replica server should be replica")
		}

		// Get the replica address for the test message
		replicaAddress, err := replicaServer.App.Cluster.Node().Address()
		if err != nil {
			t.Fatalf("failed to get replica address: %v", err)
		}

		// Create a test message from the replica to the primary
		testMessage := messages.NodeMessage{
			Data: messages.NodeConnectionMessage{
				Address: replicaAddress,
				ID:      []byte("test-replica-id"),
			},
		}

		// Encode the message to send in the request body
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err = encoder.Encode(testMessage)
		if err != nil {
			t.Fatalf("failed to encode test message: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			"/cluster/primary",
			&buf,
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.Header.Set("Content-Type", "application/gob")

		req := appHttp.NewRequest(
			primaryServer.App.Cluster,
			primaryServer.App.DatabaseManager,
			primaryServer.App.LogManager,
			request,
		)

		res := appHttp.ClusterPrimaryController(req)

		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected status code 200, got %d", res.StatusCode)
		}

		responseWriter := httptest.NewRecorder()

		// Stream the response and verify data can be received
		res.Stream(responseWriter)

		// Verify headers are set correctly for streaming
		if responseWriter.Header().Get("Content-Type") != "application/gob" {
			t.Errorf("expected Content-Type 'application/gob', got '%s'", responseWriter.Header().Get("Content-Type"))
		}

		if responseWriter.Header().Get("Transfer-Encoding") != "chunked" {
			t.Errorf("expected Transfer-Encoding 'chunked', got '%s'", responseWriter.Header().Get("Transfer-Encoding"))
		}

		// Decode the response message to verify processing worked
		responseBody := responseWriter.Body.Bytes()
		if len(responseBody) > 0 {
			decoder := gob.NewDecoder(bytes.NewReader(responseBody))
			var responseMessage messages.NodeMessage
			err := decoder.Decode(&responseMessage)

			if err != nil {
				t.Fatalf("failed to decode response message: %v", err)
			}

			// Verify we received a node connection response (echo of the ID)
			if connectionResponse, ok := responseMessage.Data.(messages.NodeConnectionMessage); ok {
				if string(connectionResponse.ID) != string(testMessage.Data.(messages.NodeConnectionMessage).ID) {
					t.Errorf("expected echo of connection ID '%s', got '%s'",
						string(testMessage.Data.(messages.NodeConnectionMessage).ID),
						string(connectionResponse.ID))
				}
			} else {
				t.Errorf("expected NodeConnectionMessage response, got %T", responseMessage.Data)
			}
		} else {
			t.Error("expected response body to contain data, but it was empty")
		}
	})
}

func TestClusterPrimaryController_NonPrimaryNode(t *testing.T) {
	test.Run(t, func() {
		// Create a primary server
		primaryServer := test.NewTestServer(t)
		defer primaryServer.Shutdown()

		// Create a replica server
		replicaServer := test.NewTestServer(t)
		defer replicaServer.Shutdown()

		// Verify the replica is not primary
		if replicaServer.App.Cluster.Node().IsPrimary() {
			t.Fatalf("replica server should not be primary")
		}

		// Create a test message
		testMessage := messages.NodeMessage{
			Data: messages.NodeConnectionMessage{
				Address: "test-node",
				ID:      []byte("test-id"),
			},
		}

		// Encode the message to send in the request body
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err := encoder.Encode(testMessage)
		if err != nil {
			t.Fatalf("failed to encode test message: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			"/cluster/primary",
			&buf,
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.Header.Set("Content-Type", "application/gob")

		// Try to call the primary controller on the replica server (should fail)
		req := appHttp.NewRequest(
			replicaServer.App.Cluster,
			replicaServer.App.DatabaseManager,
			replicaServer.App.LogManager,
			request,
		)

		res := appHttp.ClusterPrimaryController(req)

		// Should return 403 Forbidden since the node is not primary
		if res.StatusCode != http.StatusForbidden {
			t.Fatalf("expected status code 403 for non-primary node, got %d", res.StatusCode)
		}
	})
}
