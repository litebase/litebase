package test

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/cmd"
	"github.com/litebase/litebase/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type TestCLI struct {
	AccessKey    *auth.AccessKey
	App          *server.App
	Cmd          *cobra.Command
	outputBuffer *bytes.Buffer
	Server       *TestServer
}

func NewTestCLI(app *server.App) *TestCLI {
	c := &TestCLI{
		App:          app,
		outputBuffer: bytes.NewBuffer(make([]byte, 0)),
	}

	configPath := fmt.Sprintf("%s/.litebase-cli/config.json", c.App.Config.DataPath)

	cmd, err := cmd.RootCmd(configPath)

	if err != nil {
		panic(err)
	}

	cmd.SetOut(c.outputBuffer)

	c.Cmd = cmd

	return c
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
	args = append(args, "--no-interaction")

	c.Cmd.SetArgs(args)

	defer c.ResetFlagsRecursive(c.Cmd)

	return c.Cmd.Execute()
}

// Check if the output buffer contains the expected text
func (c *TestCLI) DoesntSee(text string) bool {
	return !c.Sees(text)
}

// Reset all flags to their default values after running the command.
// This prevents flag values from persisting between command runs.
func (c *TestCLI) ResetFlagsRecursive(cmd *cobra.Command) {
	// Reset flags for this command
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			f.Value.Set(f.DefValue)
			f.Changed = false
		}
	})

	// Reset flags for persistent flags (if needed)
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			f.Value.Set(f.DefValue)
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

// WithBasicAuth sets the username and password for basic authentication
func (c *TestCLI) WithBasicAuth(username, password string, statements []auth.Statement) *TestCLI {
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
