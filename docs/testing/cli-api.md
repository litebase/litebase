# Testing the CLI

This document describes the `TestCLI` API that supports testing CLI commands. It supports running both short-lived and long-running processes with real-time output processing and cancellation capabilities.

## Key Features

1. **Background Process Execution**: Run CLI commands that block (like `start`) without blocking the test
2. **Real-Time Output Processing**: Process output as it's generated with custom handlers
3. **Process Cancellation**: Cancel long-running processes during tests
4. **Output Monitoring**: Wait for specific output patterns with timeouts
5. **Thread-Safe**: All operations are thread-safe with proper synchronization
6. **Authentication Support**: Built-in support for access keys, tokens, and basic authentication
7. **Output Parsing**: Helper methods for parsing structured output from CLI commands

## API Overview

### TestCLI Creation

```go
func NewTestCLI(t *testing.T, app *server.App) *TestCLI
```

Creates a new TestCLI instance. The `app` parameter can be nil for standalone CLI testing.

### ProcessHandle

The `ProcessHandle` type represents a running long-lived process and provides these methods:

- `IsRunning() bool` - Check if the process is currently running
- `Cancel() error` - Stop the running process (with 5-second timeout)
- `Wait() error` - Wait for the process to complete or be cancelled
- `GetOutput() string` - Get all output captured so far
- `GetOutputChan() <-chan string` - Get a channel for real-time output lines (buffered, non-blocking)
- `WaitForOutput(expectedText string, timeout time.Duration) error` - Wait for specific output to appear
- `WaitForOutputLine(prefix string, timeout time.Duration) (string, error)` - Wait for and extract a line with specific prefix

### TestCLI Methods

#### Core Execution Methods

```go
func (c *TestCLI) Run(args ...string) error
```

Executes a CLI command synchronously with the provided arguments. Automatically appends `--interactive=false` to prevent interactive prompts during tests.

```go
func (c *TestCLI) RunInBackground(handler func(p *ProcessHandle)) error
```

Executes a long-running CLI command as a separate subprocess. The handler function is called with the ProcessHandle and should use it to monitor and control the process.

#### Configuration and Setup Methods

```go
func (c *TestCLI) WithArgs(args ...string) *TestCLI
```

Sets the command-line arguments that will be used with `RunInBackground`. Returns the TestCLI for method chaining.

```go
func (c *TestCLI) WithAccessKey(statements []auth.Statement) *TestCLI
```

Creates an access key with the specified authorization statements and configures the CLI to use it. Returns the TestCLI for method chaining.

```go
func (c *TestCLI) WithBasicAuth(username, password string, statements []auth.Statement) *TestCLI
```

Creates a user with the specified credentials and authorization statements, then configures the CLI to use basic authentication. Returns the TestCLI for method chaining.

```go
func (c *TestCLI) WithToken(statements []auth.Statement) *TestCLI
```

Creates a bearer token with the specified authorization statements and configures the CLI to use token-based authentication. Returns the TestCLI for method chaining.

```go
func (c *TestCLI) WithServer(server *TestServer) *TestCLI
```

Configures the CLI to connect to the specified test server. Returns the TestCLI for method chaining.

#### Output Analysis Methods

```go
func (c *TestCLI) GetOutput() string
```

Returns the current output buffer content for debugging and analysis.

```go
func (c *TestCLI) GetOutputLine(prefix string) string
```

Extracts a line from the CLI output that starts with the specified prefix followed by ": ". Returns the text after the colon, with whitespace trimmed. Useful for parsing structured output like "Port: 8080".

```go
func (c *TestCLI) Sees(text string) bool
```

Checks if the output buffer contains the specified text.

```go
func (c *TestCLI) DoesNotSee(text string) bool
```

Checks if the output buffer does NOT contain the specified text.

```go
func (c *TestCLI) ClearOutput()
```

Resets the output buffer, clearing all captured output.

#### Process Control Methods

```go
func (c *TestCLI) Cancel() error
```

Cancels any currently running background process started with `RunInBackground`.

## Test Setup and Utilities

The CLI testing API works in conjunction with other test utilities in the `internal/test` package:

### Test Environment Setup

```go
func Run(t testing.TB, callback func())
```

Sets up a clean test environment with proper cluster configuration. This should be used to wrap most CLI tests.

### Server Testing Integration

```go
func NewTestServer(t testing.TB) *TestServer
```

Creates a test server instance that can be used with `WithServer()` for integration testing.

### Application Setup

```go
func RunWithApp(t testing.TB, callback func(*server.App))
```

Sets up a complete application instance for testing CLI commands that require a full server context.

### OutputHandler Type

```go
type OutputHandler func(line string)
```

A function that processes real-time output from a command. Used with background processes to handle output as it's generated.

## Usage Examples

### Basic CLI Command Testing

```go
func TestVersionCmd(t *testing.T) {
    test.Run(t, func() {
        cli := test.NewTestCLI(t, nil)
        
        err := cli.Run("version")
        if err != nil {
            t.Fatalf("failed to run version command: %v", err)
        }
        
        if !cli.Sees("Litebase") {
            t.Error("expected version output to contain 'Litebase'")
        }
    })
}
```

### Authentication Testing

```go
func TestWithAuthentication(t *testing.T) {
    test.Run(t, func() {
        app := test.CreateApp()
        cli := test.NewTestCLI(t, app).
            WithAccessKey([]auth.Statement{
                {Effect: auth.Allow, Action: "*", Resource: "*"},
            }).
            WithServer(test.NewTestServer(app))
        
        err := cli.Run("database", "list")
        if err != nil {
            t.Fatalf("failed to list databases: %v", err)
        }
    })
}
```

### Background Server Testing

```go
func TestStartCmd(t *testing.T) {
    test.Run(t, func() {
        cli := test.NewTestCLI(t, nil).WithArgs("start", "--port", "8083")
        
        err := cli.RunInBackground(func(handle *test.ProcessHandle) {
            // Wait for the server to start
            err := handle.WaitForOutput("Litebase Server", 5*time.Second)
            if err != nil {
                t.Fatalf("timeout waiting for server to start: %v", err)
            }
            
            // Verify the server is running
            if !handle.IsRunning() {
                t.Fatal("expected server to be running")
            }
            
            // Extract the port number from output
            port, err := handle.WaitForOutputLine("Port", 2*time.Second)
            if err != nil {
                t.Fatalf("failed to get port: %v", err)
            }
            t.Logf("Server started on port: %s", port)
            
            // Cancel the process when done
            err = handle.Cancel()
            if err != nil {
                t.Fatalf("failed to cancel server: %v", err)
            }
        })
        
        if err != nil {
            t.Fatalf("failed to run background command: %v", err)
        }
    })
}
```

### Real-Time Output Processing

```go
func TestStartCmdWithOutputProcessing(t *testing.T) {
    test.Run(t, func() {
        cli := test.NewTestCLI(t, nil).WithArgs("start", "--port", "8082")
        
        var nodeID string
        
        err := cli.RunInBackground(func(handle *test.ProcessHandle) {
            // Process output in real-time
            go func() {
                for line := range handle.GetOutputChan() {
                    if strings.Contains(line, "Node ID") {
                        nodeID = strings.TrimSpace(strings.Split(line, ":")[1])
                        t.Logf("Captured Node ID: %s", nodeID)
                    }
                }
            }()
            
            // Wait for server to be ready
            err := handle.WaitForOutput("Litebase Server", 5*time.Second)
            if err != nil {
                t.Fatalf("server didn't start: %v", err)
            }
            
            // Wait a bit for the node ID to be captured
            time.Sleep(100 * time.Millisecond)
            
            if nodeID == "" {
                t.Error("failed to capture node ID")
            }
            
            handle.Cancel()
        })
        
        if err != nil {
            t.Fatalf("failed to start command: %v", err)
        }
    })
}
```

### Output Parsing and Validation

```go
func TestDatabaseCreate(t *testing.T) {
    test.Run(t, func() {
        app := test.CreateApp()
        cli := test.NewTestCLI(t, app).
            WithAccessKey([]auth.Statement{
                {Effect: auth.Allow, Action: "*", Resource: "*"},
            })
        
        err := cli.Run("database", "create", "testdb")
        if err != nil {
            t.Fatalf("failed to create database: %v", err)
        }
        
        // Check that the database was created successfully
        if !cli.Sees("Database created successfully") {
            t.Error("expected success message")
        }
        
        // Extract database ID from output
        dbID := cli.GetOutputLine("Database ID")
        if dbID == "" {
            t.Error("expected database ID in output")
        }
        
        t.Logf("Created database with ID: %s", dbID)
        
        // Verify we can list the database
        cli.ClearOutput()
        err = cli.Run("database", "list")
        if err != nil {
            t.Fatalf("failed to list databases: %v", err)
        }
        
        if !cli.Sees("testdb") {
            t.Error("expected to see created database in list")
        }
    })
}
```

## Important Notes

1. **Background Process Management**: The `RunInBackground` method uses a subprocess approach via `go run` for better isolation. The handler function is called with a ProcessHandle to manage the process lifecycle.

2. **Automatic Flag Handling**: All commands automatically have `--interactive=false` appended to prevent interactive prompts during tests.

3. **Port Conflicts**: When testing multiple servers, use different ports to avoid conflicts (e.g., `--port 8081`, `--port 8082`).

4. **Output Channels**: The output channel is buffered (100 lines) and non-blocking. If the channel fills up, output lines may be skipped to prevent blocking.

5. **Thread Safety**: All methods are thread-safe and can be called from multiple goroutines.

6. **Resource Cleanup**: Channels and resources are automatically cleaned up when the process completes or is cancelled. The `Cancel()` method has a 5-second timeout.

7. **Flag Reset**: Flags are automatically reset after each `Run()` call to prevent values from persisting between command runs.

8. **ASCII Filtering**: Output parsing methods (`GetOutputLine`, `WaitForOutputLine`) filter non-ASCII characters to handle terminal escape sequences.

9. **Method Chaining**: Configuration methods (`WithArgs`, `WithAccessKey`, etc.) return the TestCLI instance for convenient method chaining.

10. **Error Handling**: Background processes capture both stdout and stderr. Errors are reported through the ProcessHandle's error channel.

## Best Practices

1. **Use Method Chaining**: Configure your CLI instance in a fluent style:

   ```go
   cli := test.NewTestCLI(t, app).
       WithAccessKey(statements).
       WithServer(server).
       WithArgs("start", "--port", "8080")
   ```

2. **Always Handle Cancellation**: For background processes, always ensure you cancel them:

   ```go
   err := cli.RunInBackground(func(handle *test.ProcessHandle) {
       defer handle.Cancel() // Ensure cleanup
       
       // Your test logic here
   })
   ```

3. **Use Timeouts**: Always use timeouts when waiting for output to prevent hanging tests:

   ```go
   err := handle.WaitForOutput("Server started", 5*time.Second)
   ```

4. **Check Running Status**: Verify process state before performing operations:

   ```go
   if !handle.IsRunning() {
       t.Fatal("expected process to be running")
   }
   ```

5. **Clear Output Between Tests**: Use `ClearOutput()` when reusing CLI instances:

   ```go
   cli.ClearOutput()
   err := cli.Run("next", "command")
   ```

This enhanced API provides comprehensive support for testing both simple CLI commands and complex long-running processes while maintaining thread safety and proper resource management.
