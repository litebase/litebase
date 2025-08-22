//go:build unix

package test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// createListener creates a Unix domain socket listener
func createListener(socketDir string) (net.Listener, string, error) {
	socketPath := filepath.Join(socketDir, "coordinator.sock")

	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, "", fmt.Errorf("error removing socket file: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create Unix socket listener: %v", err)
	}

	return listener, socketPath, nil
}

// connectToSocket connects to a Unix domain socket
func connectToSocket(socketPath string) (net.Conn, error) {
	return net.Dial("unix", socketPath)
}

// getSocketPath returns the socket path for the coordinator
func getSocketPath(socketDir string) string {
	return filepath.Join(socketDir, "coordinator.sock")
}

// cleanupSocket removes the socket file
func cleanupSocket(socketPath string) error {
	return os.Remove(socketPath)
}
