package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestHealthCheckController(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		err := server1.App.Cluster.Node().Primary().ValidateReplica(fmt.Sprintf("%s/health", server2.Address))

		if err != nil {
			t.Fatalf("failed to validate replica: %v", err)
		}
	})
}
