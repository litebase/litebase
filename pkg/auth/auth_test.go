package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestAuthBroadcast(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		auth := server.App.Auth

		var key, value string

		auth.Broadcaster(func(k string, v string) {
			key = k
			value = v
		})

		auth.Broadcast("testKey", "testValue")

		if key != "testKey" || value != "testValue" {
			t.Fatalf("Expected broadcast to be 'testKey: testValue', got '%s: %s'", key, value)
		}
	})
}
