//go:build windows

package test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// createListener creates a TCP listener on Windows (since Unix sockets aren't supported)
func createListener(socketDir string) (net.Listener, string, error) {
	// On Windows, we use TCP instead of Unix domain sockets
	// Listen on localhost with port 0 to get an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", fmt.Errorf("failed to create TCP listener: %v", err)
	}

	// Get the actual port that was assigned
	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port

	// Create a "socket path" that's actually the address for consistency
	socketPath := fmt.Sprintf("127.0.0.1:%d", port)

	// Write the port to a file in the socket directory for child processes to read
	portFile := filepath.Join(socketDir, "coordinator.port")
	err = writePortFile(portFile, port)
	if err != nil {
		listener.Close()
		return nil, "", fmt.Errorf("failed to write port file: %v", err)
	}

	return listener, socketPath, nil
}

// connectToSocket connects to a TCP socket on Windows
func connectToSocket(socketPath string) (net.Conn, error) {
	// socketPath is actually a TCP address on Windows
	return net.Dial("tcp", socketPath)
}

// getSocketPath returns the socket path for the coordinator on Windows
func getSocketPath(socketDir string) string {
	// Read the port from the port file
	portFile := filepath.Join(socketDir, "coordinator.port")
	port, err := readPortFile(portFile)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// writePortFile writes the port number to a file
func writePortFile(filename string, port int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(port))
	return err
}

// readPortFile reads the port number from a file
func readPortFile(filename string) (int, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(data))
}
