package test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/cmd"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/litebase/litebase/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type TestCLI struct {
	AccessKey     *auth.AccessKey
	args          []string
	App           *server.App
	Cmd           *cobra.Command
	outputBuffer  *bytes.Buffer
	processHandle *ProcessHandle
	Server        *TestServer
	t             *testing.T
	Token         *auth.Token
}

// OutputHandler is a function that processes real-time output from a command
type OutputHandler func(line string)

func NewTestCLI(t *testing.T, app *server.App) *TestCLI {
	c := &TestCLI{
		App:          app,
		outputBuffer: bytes.NewBuffer(make([]byte, 0)),
		t:            t,
	}

	var command *cobra.Command
	configPath := ""
	var err error

	if c.App != nil {
		configPath = fmt.Sprintf("%s/.litebase/config.yml", c.App.Config.StorageLocalPath)

		_, err := config.NewConfiguration(configPath, true)

		if err != nil {
			c.t.Fatalf("failed to load config: %v", err)
			return nil
		}
	}

	command, err = cmd.RootCmd(configPath)

	if err != nil {
		panic(err)
	}

	command.SetOut(c.outputBuffer)

	c.Cmd = command
	c.Cmd.Version = "test"

	return c
}

// Cancel the running CLI.
func (c *TestCLI) Cancel() error {
	if c.processHandle != nil {
		return c.processHandle.Cancel()
	}

	return nil
}

// ClearOutput resets the output buffer for the CLI
func (c *TestCLI) ClearOutput() {
	c.outputBuffer.Reset()
}

// Get a line of text from the CLI output that is prefixed by the follow text
// returning the text following a colon.
func (c *TestCLI) GetOutputLine(prefix string) string {
	lines := bytes.SplitSeq(c.outputBuffer.Bytes(), []byte("\n"))

	for line := range lines {
		// Remove non-standard (non-ASCII) characters from the line
		cleanLine := make([]byte, 0, len(line))

		for _, b := range line {
			if b >= 32 && b <= 126 { // ASCII printable range
				cleanLine = append(cleanLine, b)
			}
		}

		if bytes.HasPrefix(cleanLine, []byte(prefix+": ")) {
			result := string(cleanLine[len(prefix)+2:])
			// Trim whitespace from the result to avoid issues with padding
			return strings.TrimSpace(result)
		}
	}

	return ""
}

// GetOutput returns the current output buffer content for debugging
func (c *TestCLI) GetOutput() string {
	return c.outputBuffer.String()
}

// Run executes the CLI command with the provided arguments
func (c *TestCLI) Run(args ...string) error {
	args = append(args, "--interactive=false")

	c.Cmd.SetArgs(args)

	defer c.ResetFlagsRecursive(c.Cmd)

	return c.Cmd.Execute()
}

// RunBackground executes a long-running CLI command in the background and
// returns a ProcessHandle that can be used to monitor output and cancel the
// process.
func (c *TestCLI) RunInBackground(handler func(p *ProcessHandle)) error {
	// Use context.Background() instead of cancellable context to prevent automatic cancellation
	ctx := context.Background()

	// Create a new process handle with its own cancellable context
	handle := NewProcessHandle(ctx)

	// Prepare the command arguments
	cmdArgs := append([]string{"run", "./../../../cmd/litebase"}, c.args...) // Use -run ^$ to run no tests, just the CLI
	cmdArgs = append(cmdArgs, "--interactive=false")

	// Create the command WITHOUT context so it doesn't get auto-cancelled
	cmd := exec.Command("go", cmdArgs...)
	cmd.Dir, _ = os.Getwd()

	// Set up process group for proper signal handling (cross-platform)
	setupProcessGroup(cmd)

	// Set up environment variables to run the CLI command
	cmd.Env = append(os.Environ(), "LITEBASE_TEST_CLI_MODE=true")

	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()

	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()

	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	handle.cmd = cmd

	// Store the handle for cancellation
	c.processHandle = handle

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	handle.mutex.Lock()
	handle.isRunning = true
	handle.mutex.Unlock()

	// Start goroutines to handle output
	go c.handleOutput(handle, stdout)
	go c.handleOutput(handle, stderr)

	// Start goroutine to wait for command completion
	go func() {
		err := cmd.Wait()

		// Update the running status after the command has actually exited
		handle.mutex.Lock()
		handle.isRunning = false
		handle.mutex.Unlock()

		if err != nil && handle.ctx.Err() == nil {
			select {
			case handle.errorChan <- err:
			default:
			}
		}

		// Close channels and mark as done
		handle.closeOnce.Do(func() {
			handle.mutex.Lock()
			handle.closed = true
			handle.mutex.Unlock()
			close(handle.doneChan)
			close(handle.outputChan)
		})
	}()

	handler(handle)

	<-handle.doneChan

	return nil
}

// handleOutput processes output from a reader and sends it to the output handler and channels
func (c *TestCLI) handleOutput(handle *ProcessHandle, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()

		// Store the output
		handle.mutex.Lock()
		handle.output.WriteString(line + "\n")
		isClosed := handle.closed
		handle.mutex.Unlock()

		// Don't try to send if we're closed
		if !isClosed {
			// Send to output channel (non-blocking)
			select {
			case handle.outputChan <- line:
			case <-handle.ctx.Done():
				return
			default:
				// Channel is full or blocked, skip this line to avoid blocking
			}
		}
	}
}

// Check if the output buffer contains the expected text
func (c *TestCLI) DoesNotSee(text string) bool {
	return !c.Sees(text)
}

// Reset all flags to their default values after running the command.
// This prevents flag values from persisting between command runs.
func (c *TestCLI) ResetFlagsRecursive(cmd *cobra.Command) {
	// Reset flags for this command
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			if err := f.Value.Set(f.DefValue); err != nil {
				slog.Error("Error resetting flag value:", "error", err)
			}

			f.Changed = false
		}
	})

	// Reset flags for persistent flags (if needed)
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			if err := f.Value.Set(f.DefValue); err != nil {
				slog.Error("Error resetting flag value:", "error", err)
			}

			f.Changed = false
		}
	})

	// Recurse into child commands
	for _, child := range cmd.Commands() {
		c.ResetFlagsRecursive(child)
	}
}

// Check if the output buffer does not contain the expected text
func (c *TestCLI) Sees(text string) bool {
	return bytes.Contains(c.outputBuffer.Bytes(), []byte(text))
}

// WithAccessKey sets the access key for the CLI and updates the flags
func (c *TestCLI) WithAccessKey(statements []auth.Statement) *TestCLI {
	// Wait for primary to ensure migrations have run
	if err := c.App.Cluster.Node().WaitForPrimary(); err != nil {
		panic(fmt.Errorf("failed to wait for primary: %w", err))
	}

	accessKey, err := c.App.Auth.AccessKeyManager.Create("Test access key", statements)

	if err != nil {
		panic(err)
	}

	c.AccessKey = accessKey

	err = c.Cmd.PersistentFlags().Set("access-key-id", accessKey.AccessKeyID)

	if err != nil {
		panic(err)
	}

	err = c.Cmd.PersistentFlags().Set("access-key-secret", accessKey.AccessKeySecret)

	if err != nil {
		panic(err)
	}

	return c
}

// WithArgs sets the command-line arguments for the CLI command.
func (c *TestCLI) WithArgs(args ...string) *TestCLI {
	c.args = args

	return c
}

// WithBasicAuth sets the username and password for basic authentication
func (c *TestCLI) WithBasicAuth(username, password string, statements []auth.Statement) *TestCLI {
	// Wait for primary to ensure migrations have run
	if err := c.App.Cluster.Node().WaitForPrimary(); err != nil {
		panic(fmt.Errorf("failed to wait for primary: %w", err))
	}

	_, err := c.App.Auth.UserManager.Create(username, password, "", statements)

	if err != nil {
		panic(err)
	}

	err = c.Cmd.PersistentFlags().Set("username", username)

	if err != nil {
		panic(err)
	}

	err = c.Cmd.PersistentFlags().Set("password", password)

	if err != nil {
		panic(err)
	}

	return c
}

// WithServer sets the server for the CLI and updates the URL flag
func (c *TestCLI) WithServer(server *TestServer) *TestCLI {
	c.Server = server

	err := c.Cmd.PersistentFlags().Set("url", server.Server.URL)

	if err != nil {
		panic(err)
	}

	return c
}

// WithToken sets the bearer token for the CLI and updates the Authorization header
func (c *TestCLI) WithToken(statements []auth.Statement) *TestCLI {
	// Wait for primary to ensure migrations have run
	if err := c.App.Cluster.Node().WaitForPrimary(); err != nil {
		panic(fmt.Errorf("failed to wait for primary: %w", err))
	}

	token, err := c.App.Auth.TokenManager.Create("Test token", statements)

	if err != nil {
		slog.Error("Error creating token:", "error", err)
		return c
	}

	c.Token = token

	tokenValue, err := token.Value()

	if err != nil {
		return c
	}

	err = c.Cmd.PersistentFlags().Set("token", tokenValue)

	if err != nil {
		panic(err)
	}

	return c
}
