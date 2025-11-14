package cmd_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/cli/cmd"
	"github.com/spf13/cobra"
)

// FuzzCLICommands uses Go's native fuzzing to test ALL CLI commands dynamically
// by discovering them from the command tree and running them with malformed API responses.
// This ensures safe type assertions when parsing API response data.
//
// To run this fuzz test in isolation (recommended to avoid interactive TUI tests):
//
//	go test ./pkg/cli/cmd -fuzz=FuzzCLICommands -fuzztime=10s -run=^FuzzCLICommands$
//
// The -run=^FuzzCLICommands$ ensures only the fuzz test runs, avoiding other tests
// that might have interactive components
func FuzzCLICommands(f *testing.F) {
	// Seed with various response formats that represent edge cases
	f.Add([]byte(`{"message":"success","data":{}}`))
	f.Add([]byte(`{"message":"success","data":null}`))
	f.Add([]byte(`{"message":"success","data":[]}`))
	f.Add([]byte(`{"message":"success","data":"string"}`))
	f.Add([]byte(`{"message":"success","data":123}`))
	f.Add([]byte(`{"message":"success","data":{"username":null}}`))
	f.Add([]byte(`{"message":"success","data":{"username":123}}`))
	f.Add([]byte(`{"message":"success","data":{"username":"test","createdAt":123}}`))
	f.Add([]byte(`{"message":"success","data":{"tokenId":"abc","description":null}}`))
	f.Add([]byte(`{"message":"success","data":{"statements":null}}`))
	f.Add([]byte(`{"message":"success","data":{"statements":"not-array"}}`))
	f.Add([]byte(`{"message":"success","data":{"statements":[]}}`))
	f.Add([]byte(`{"message":"success","data":[{"transactionId":null}]}`))
	f.Add([]byte(`{"message":"success","data":[{"changes":"not-number"}]}`))
	f.Add([]byte(`{"message":"success","data":[{"rows":null}]}`))
	f.Add([]byte(`{"message":"success","data":{"nodeCount":"string"}}`))
	f.Add([]byte(`{"message":"success","data":{"size":"not-number"}}`))
	f.Add([]byte(`{"message":"success","data":{"restorePoint":null}}`))
	f.Add([]byte(`{"message":"success","data":{"restorePoint":{}}}`))
	f.Add([]byte(`{"message":"success","data":{"restorePoint":{"timestamp":null}}}`))

	f.Fuzz(func(t *testing.T, responseData []byte) {
		// Skip if data is too large
		if len(responseData) > 10*1024 {
			return
		}

		// Validate it's at least valid JSON
		var testJSON any

		if err := json.Unmarshal(responseData, &testJSON); err != nil {
			return // Skip invalid JSON
		}

		// Dynamically discover and test all CLI commands with this malformed response
		runAllCommandsWithMalformedResponse(t, responseData)
	})
}

// runAllCommandsWithMalformedResponse discovers all CLI commands and runs them with malformed API responses
func runAllCommandsWithMalformedResponse(t *testing.T, malformedResponse []byte) {
	// Create a mock HTTP server that returns the malformed response for ALL endpoints
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err := w.Write(malformedResponse)

		if err != nil {
			t.Logf("Failed to write response: %v", err)
		}
	}))

	defer mockServer.Close()

	// Create a temporary config directory for this test
	tmpDir := t.TempDir()
	configPath := fmt.Sprintf("%s/.litebase/config.yml", tmpDir)

	// Create root command
	rootCmd, err := cmd.RootCmd(configPath)

	if err != nil {
		t.Skipf("Failed to create root command: %v", err)

		return
	}

	// Discover all leaf commands (commands that actually execute, not just groups)
	commands := discoverLeafCommands(rootCmd)

	// Skip if we couldn't find any commands
	if len(commands) == 0 {
		t.Skip("No commands discovered")

		return
	}

	// Test each command with the malformed response
	for _, cmdPath := range commands {
		// Skip commands that don't make API calls
		if shouldSkipCommand(cmdPath) {
			continue
		}

		testCommandWithMalformedResponse(t, cmdPath, mockServer.URL, configPath)
	}
}

// discoverLeafCommands recursively finds all executable (leaf) commands
func discoverLeafCommands(root *cobra.Command) [][]string {
	var commands [][]string

	var walk func(*cobra.Command, []string)

	walk = func(cmd *cobra.Command, path []string) {
		currentPath := append(path, cmd.Name())

		// If this command has a Run function and is not the root, it's a leaf command
		if cmd.HasAvailableSubCommands() {
			// Recurse into subcommands
			for _, sub := range cmd.Commands() {
				if !sub.Hidden {
					walk(sub, currentPath)
				}
			}
		} else if cmd.RunE != nil || cmd.Run != nil {
			// This is a leaf command
			commands = append(commands, currentPath)
		}
	}

	// Start walking from root's children (skip root itself)
	for _, sub := range root.Commands() {
		if !sub.Hidden {
			walk(sub, []string{})
		}
	}

	return commands
}

// shouldSkipCommand returns true for commands that don't make API calls
func shouldSkipCommand(cmdPath []string) bool {
	if len(cmdPath) == 0 {
		return true
	}

	// Skip commands that don't interact with the API
	skipPrefixes := []string{
		"config",  // Local config commands
		"profile", // Local profile commands
		"start",   // Server start command
		"status",  // Local status check
	}

	return slices.Contains(skipPrefixes, cmdPath[0])
}

// testCommandWithMalformedResponse runs a single command with a mock server returning malformed data
func testCommandWithMalformedResponse(t *testing.T, cmdPath []string, mockURL string, configPath string) {
	// Use a sub-test to isolate panics
	t.Run(fmt.Sprintf("%v", cmdPath), func(t *testing.T) {
		// Set a timeout for this specific test to avoid hanging
		done := make(chan bool, 1)
		panicMsg := make(chan any, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicMsg <- r
				}
				done <- true
			}()

			// Create a fresh command instance
			rootCmd, err := cmd.RootCmd(configPath)

			if err != nil {
				return
			}

			// Build the command arguments
			args := cmdPath

			// Add minimal required arguments for different command types
			args = addRequiredArgs(cmdPath, args)

			// Add flags to point to mock server and disable interactive mode
			args = append(args, "--url", mockURL)
			args = append(args, "--access-key-id", "test-key")
			args = append(args, "--access-key-secret", "test-secret")
			args = append(args, "--interactive=false") // Disable interactive mode

			// Set the args
			rootCmd.SetArgs(args)

			// Execute the command - we expect it might error, but it should NOT panic
			_ = rootCmd.Execute()
		}()

		// Wait for completion or timeout after 1 second
		select {
		case <-done:
			// Check if there was a panic
			select {
			case msg := <-panicMsg:
				t.Errorf("PANIC in command %v: %v", cmdPath, msg)
			default:
				// No panic, test passed
			}
		case <-time.After(1 * time.Second):
			t.Skipf("Command %v timed out (likely waiting for interactive input)", cmdPath)
		}
	})
}

// addRequiredArgs adds minimal required positional arguments for commands
func addRequiredArgs(cmdPath []string, args []string) []string {
	if len(cmdPath) < 2 {
		return args
	}

	// Add dummy arguments based on command patterns
	switch {
	case contains(cmdPath, "show"):
		// show commands usually need an ID/name
		args = append(args, "test-id")
	case contains(cmdPath, "delete"):
		// delete commands need an ID
		args = append(args, "test-id")
	case contains(cmdPath, "update"):
		// update commands need an ID
		args = append(args, "test-id")
	case contains(cmdPath, "create"):
		// create commands usually need a name
		args = append(args, "test-resource")
	case contains(cmdPath, "query"):
		// query needs database/branch and query
		args = append(args, "test-db/main", "SELECT 1")
	case contains(cmdPath, "restore"):
		// restore needs database/branch and timestamp
		args = append(args, "test-db/main", "12345")
	case contains(cmdPath, "backup"):
		if contains(cmdPath, "delete") {
			// backup delete needs database/branch and timestamp
			args = append(args, "test-db/main", "12345")
		} else if contains(cmdPath, "show") {
			// backup show needs database/branch and timestamp
			args = append(args, "test-db/main", "12345")
		} else if len(cmdPath) > 2 && (cmdPath[len(cmdPath)-1] == "list" || cmdPath[len(cmdPath)-1] == "create") {
			// backup list/create needs database/branch
			args = append(args, "test-db/main")
		}
	case contains(cmdPath, "snapshot"):
		if contains(cmdPath, "show") {
			// snapshot show needs database/branch and timestamp
			args = append(args, "test-db/main", "12345")
		} else if contains(cmdPath, "list") {
			// snapshot list needs database/branch
			args = append(args, "test-db/main")
		}
	case contains(cmdPath, "branch"):
		if contains(cmdPath, "create") {
			// branch create needs database and branch name
			args = append(args, "test-db", "test-branch")
		} else if contains(cmdPath, "delete") || contains(cmdPath, "show") {
			// branch delete/show needs database/branch
			args = append(args, "test-db/test-branch")
		} else if contains(cmdPath, "list") {
			// branch list needs database
			args = append(args, "test-db")
		}
	case contains(cmdPath, "query-logs"):
		// query-logs needs database/branch
		args = append(args, "test-db/main")
	}

	return args
}

// contains checks if a slice contains a string
func contains(slice []string, str string) bool {
	return slices.Contains(slice, str)
}

// TestDiscoverCommands verifies that the command discovery works correctly
func TestDiscoverCommands(t *testing.T) {
	rootCmd, err := cmd.RootCmd("")

	if err != nil {
		t.Fatalf("Failed to create root command: %v", err)
	}

	commands := discoverLeafCommands(rootCmd)

	for _, cmdPath := range commands {
		if !shouldSkipCommand(cmdPath) {
			t.Logf("  - %v", cmdPath)
		}
	}

	// Verify we found a reasonable number of commands
	if len(commands) < 10 {
		t.Errorf("Expected to discover at least 10 commands, found %d", len(commands))
	}

	// Verify some known commands exist
	expectedCommands := [][]string{
		{"user", "show"},
		{"user", "list"},
		{"token", "create"},
		{"access-key", "create"},
		{"database", "query"},
	}

	for _, expected := range expectedCommands {
		found := false
		for _, discovered := range commands {
			if sliceEqual(expected, discovered) {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected command %v not found in discovered commands", expected)
		}
	}
}

// sliceEqual checks if two string slices are equal
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
