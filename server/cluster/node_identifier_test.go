package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/server/cluster"
)

func TestNewNodeIdentifier(t *testing.T) {
	identifier := cluster.NewNodeIdentifier("127.0.0.1:8080", 1)

	if identifier.Address != "127.0.0.1:8080" {
		t.Error("Address not set correctly")
	}

	if identifier.ID != 1 {
		t.Error("ID not set correctly")
	}
}
