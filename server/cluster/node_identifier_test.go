package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/server/cluster"
)

func TestNewNodeIdentifier(t *testing.T) {
	identifier := cluster.NewNodeIdentifier("127.0.0.1", "8080")

	if identifier.Address != "127.0.0.1" {
		t.Error("Address not set correctly")
	}

	if identifier.Port != "8080" {
		t.Error("Port not set correctly")
	}

	if identifier.String() != "127.0.0.1:8080" {
		t.Error("String() not returning expected value")
	}
}
