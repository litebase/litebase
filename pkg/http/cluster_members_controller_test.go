package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestClusterMemberDestroyController(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		address, err := server2.App.Cluster.Node().Address()

		if err != nil {
			t.Fatalf("failed to get server address: %v", err)
		}

		request, err := http.NewRequest(
			"DELETE",
			fmt.Sprintf("/cluster/members/%s", address),
			nil,
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		encryptedHeader, err := server2.App.Cluster.Auth.SecretsManager.Encrypt(
			server2.App.Cluster.Config.EncryptionKey,
			[]byte(address),
		)

		if err != nil {
			t.Fatalf("failed to encrypt header: %v", err)
		}

		request.SetPathValue("address", address)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Lbdb-Node", string(encryptedHeader))

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterMemberDestroyController(req)

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status code 200, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}

func TestClusterMemberDestroyControllerUnauthorized(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		address, err := server2.App.Cluster.Node().Address()

		if err != nil {
			t.Fatalf("failed to get server address: %v", err)
		}

		request, err := http.NewRequest(
			"DELETE",
			fmt.Sprintf("/cluster/members/%s", address),
			nil,
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.SetPathValue("address", "invalid-address")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Lbdb-Node", "invalid-header")

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterMemberDestroyController(req)

		if res.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status code 401, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}

func TestClusterMemberStoreController(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		address, err := server2.App.Cluster.Node().Address()

		if err != nil {
			t.Fatalf("failed to get server address: %v", err)
		}

		jsonData, err := json.Marshal(&appHttp.ClusterMemberStoreRequest{
			ID:      server2.App.Cluster.Node().ID,
			Address: address,
		})

		if err != nil {
			t.Fatalf("failed to marshal request data: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			fmt.Sprintf("/cluster/members/%s", address),
			bytes.NewReader(jsonData),
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		encryptedHeader, err := server2.App.Cluster.Auth.SecretsManager.Encrypt(
			server2.App.Cluster.Config.EncryptionKey,
			[]byte(address),
		)

		if err != nil {
			t.Fatalf("failed to encrypt header: %v", err)
		}

		request.SetPathValue("address", address)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Lbdb-Node", string(encryptedHeader))

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterMemberStoreController(req)

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status code 200, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}

func TestClusterMemberStoreControllerUnauthorized(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		address, err := server2.App.Cluster.Node().Address()

		if err != nil {
			t.Fatalf("failed to get server address: %v", err)
		}

		jsonData, err := json.Marshal(&appHttp.ClusterMemberStoreRequest{
			ID:      server2.App.Cluster.Node().ID,
			Address: address,
		})

		if err != nil {
			t.Fatalf("failed to marshal request data: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			fmt.Sprintf("/cluster/members/%s", address),
			bytes.NewReader(jsonData),
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Lbdb-Node", "invalid-header")

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterMemberStoreController(req)

		if res.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status code 401, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}
