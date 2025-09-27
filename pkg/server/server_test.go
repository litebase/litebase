package server

import (
	"testing"

	"github.com/litebase/litebase/pkg/config"
)

func TestDynamicPortAllocation(t *testing.T) {
	// Test with port 0 (auto-assign)
	cfg := &config.Config{
		Port:        "8080",
		PrivatePort: "0", // Auto-assign
	}

	srv := NewServer(cfg)

	// Test the port finding function indirectly by checking if we can create the server
	if srv == nil {
		t.Fatal("Failed to create server")
	}

	// Test with specific port
	cfg2 := &config.Config{
		Port:        "8081",
		PrivatePort: "9091",
	}

	srv2 := NewServer(cfg2)

	if srv2 == nil {
		t.Fatal("Failed to create server with specific port")
	}
}

func TestGetPrivatePortMethods(t *testing.T) {
	cfg := &config.Config{
		Port:        "8080",
		PrivatePort: "0",
	}

	srv := NewServer(cfg)

	// Initially should be 0 since server hasn't started
	if srv.GetPrivatePort() != 0 {
		t.Errorf("Expected private port to be 0 initially, got %d", srv.GetPrivatePort())
	}

	// GetPrivateAddress should return formatted string
	expectedAddr := ":0"
	if srv.GetPrivateAddress() != expectedAddr {
		t.Errorf("Expected address %s, got %s", expectedAddr, srv.GetPrivateAddress())
	}
}
