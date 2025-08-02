package http_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestInternalMiddleware(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		request, err := http.NewRequest("GET", "/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req := appHttp.NewRequest(
			server1.App.Cluster,
			server1.App.DatabaseManager,
			server1.App.LogManager,
			request,
		)

		_, res := appHttp.Internal(req)

		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Expected status code %d, got %d", http.StatusUnauthorized, res.StatusCode)
		}

		address, _ := server2.App.Cluster.Node().Address()

		encryptedHeader, err := server2.App.Cluster.Auth.SecretsManager.Encrypt(
			server2.App.Cluster.Config.EncryptionKey,
			[]byte(address),
		)

		if err != nil {
			t.Fatalf("Failed to encrypt header: %s", err.Error())
		}

		request.Header.Set("X-Lbdb-Node", string(encryptedHeader))
		request.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UTC().UnixNano()))

		req = appHttp.NewRequest(
			server1.App.Cluster,
			server1.App.DatabaseManager,
			server1.App.LogManager,
			request,
		)

		_, res = appHttp.Internal(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}
	})
}
