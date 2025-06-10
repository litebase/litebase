package http_test

import (
	"bytes"
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster/messages"
	appHttp "github.com/litebase/litebase/server/http"
)

func TestClusterConnectionController(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test message to send through the stream
		testMessage := messages.NodeMessage{
			Data: messages.NodeConnectionMessage{
				Address: "test-node",
				ID:      []byte("test-connection-id"),
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
			"/cluster/connection",
			&buf,
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.Header.Set("Content-Type", "application/gob")

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterConnectionController(req)

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

		// Decode the response message to verify streaming worked
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

		// Verify the response was written (should contain at least some data for a successful stream)
		if responseWriter.Code == 0 {
			// httptest.ResponseRecorder doesn't set Code automatically, so check if headers were written
			if len(responseWriter.Header()) == 0 {
				t.Error("expected response headers to be written during streaming")
			}
		}
	})
}
