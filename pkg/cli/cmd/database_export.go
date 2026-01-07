package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

type ExportResponse struct {
	ID         string    `json:"id"`
	RangeCount int64     `json:"rangeCount"`
	StartedAt  time.Time `json:"startedAt"`
}

func NewDatabaseExportCmd(config *config.CLIConfiguration) *cobra.Command {
	var concurrency int

	cmd := &cobra.Command{
		Use:   "export <database>/<branch> <path>",
		Args:  cobra.ExactArgs(2),
		Short: "Export a database to a SQLite file",
		Long:  "Export a database branch to a SQLite file on the local filesystem.",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, outputPath, err := parseExportArguments(args)

			if err != nil {
				return fmt.Errorf("failed to parse arguments: %w", err)
			}

			// Create the export and keep the connection alive
			exportResponse, closeExport, err := createExport(config, databaseName, branchName)

			if err != nil {
				return fmt.Errorf("failed to create export: %w", err)
			}

			// Ensure we close the export connection when done
			defer closeExport()

			exportID := exportResponse.ID
			rangeCount := exportResponse.RangeCount

			rangeWord := "ranges"

			if rangeCount == 1 {
				rangeWord = "range"
			}

			_, err = fmt.Fprintf(
				cmd.OutOrStdout(),
				"Exporting %s/%s (%d %s) to %s...\n",
				databaseName,
				branchName,
				rangeCount,
				rangeWord,
				outputPath,
			)

			if err != nil {
				return fmt.Errorf("failed to print export message: %w", err)
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Export created with ID: %s\n", exportID)

			if err != nil {
				return fmt.Errorf("failed to print export ID message: %w", err)
			}

			// Download ranges concurrently
			tempDir := filepath.Dir(outputPath)
			baseFilename := strings.TrimSuffix(filepath.Base(outputPath), filepath.Ext(outputPath))

			if err := downloadRangesConcurrently(
				cmd,
				config,
				databaseName,
				branchName,
				exportID,
				tempDir,
				baseFilename,
				rangeCount,
				concurrency,
			); err != nil {
				return fmt.Errorf("failed to download ranges: %w", err)
			}

			// Merge ranges into final database file
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Merging ranges into %s...\n", outputPath)

			if err != nil {
				return fmt.Errorf("failed to print merge message: %w", err)
			}

			if err := mergeRanges(tempDir, baseFilename, outputPath, rangeCount); err != nil {
				return fmt.Errorf("failed to merge ranges: %w", err)
			}

			message := fmt.Sprintf(
				"Successfully exported %s/%s to %s",
				databaseName,
				branchName,
				outputPath,
			)

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(message),
				),
			)

			return err
		},
	}

	cmd.Flags().IntVarP(&concurrency, "concurrency", "n", 3, "Number of ranges to download concurrently")

	return cmd
}

// Create a new export with the Litebase API and return the response along with a close function
// The close function MUST be called when done to release the server-side export
func createExport(config *config.CLIConfiguration, databaseName string, branchName string) (*ExportResponse, func(), error) {
	path := fmt.Sprintf("/v1/databases/%s/branches/%s/export", databaseName, branchName)

	// Create API client for making the initial POST request
	client, err := api.NewClient(config)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to create API client: %w", err)
	}

	// Make HTTP request manually to handle streaming response
	url := fmt.Sprintf("%s%s", client.BaseURL, path)
	req, err := http.NewRequest("POST", url, nil)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set standard headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Add authentication using client's auth logic
	if err := applyClientAuth(client, req, "POST", path, nil); err != nil {
		return nil, nil, fmt.Errorf("failed to add auth headers: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 0, // No timeout - keep connection alive while downloading ranges
	}

	resp, err := httpClient.Do(req)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to make request: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, err := io.ReadAll(resp.Body)

		if err != nil {
			if err := resp.Body.Close(); err != nil {
				slog.Error("Failed to close response body", "error", err)
			}

			return nil, nil, fmt.Errorf("export failed with status %d and unknown body", resp.StatusCode)
		}

		if err := resp.Body.Close(); err != nil {
			slog.Error("Failed to close response body", "error", err)
		}

		return nil, nil, fmt.Errorf("export failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Read just the first JSON response (before keepalive newlines)
	decoder := json.NewDecoder(resp.Body)

	// API returns data wrapped in a response object
	var apiResponse struct {
		Data    ExportResponse `json:"data"`
		Message string         `json:"message"`
		Status  string         `json:"status"`
	}

	if err := decoder.Decode(&apiResponse); err != nil {
		if err := resp.Body.Close(); err != nil {
			slog.Error("Failed to close response body", "error", err)
		}

		return nil, nil, fmt.Errorf("failed to decode export response: %w", err)
	}

	// Return the export data and a close function that closes the response body
	// The connection will stay open on the server side until the close function is called
	closeFunc := func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("Failed to close response body", "error", err)
		}
	}

	return &apiResponse.Data, closeFunc, nil
}

// Parse the export command arguments into a database name, branch name, and output path
func parseExportArguments(args []string) (string, string, string, error) {
	if len(args) != 2 {
		return "", "", "", fmt.Errorf("invalid number of arguments: %d", len(args))
	}

	databasePath := args[0]
	outputPath := args[1]

	// Parse database/branch from path
	parts := strings.Split(databasePath, "/")

	if len(parts) != 2 {
		return "", "", "", errors.New("database path must be in format: database/branch")
	}

	databaseName := parts[0]
	branchName := parts[1]

	// Ensure output path has .sqlite extension
	if !strings.HasSuffix(outputPath, ".sqlite") {
		outputPath += ".sqlite"
	}

	return databaseName, branchName, outputPath, nil
}

// Download ranges concurrently
func downloadRangesConcurrently(
	cmd *cobra.Command,
	config *config.CLIConfiguration,
	databaseName string,
	branchName string,
	exportID string,
	tempDir string,
	baseFilename string,
	rangeCount int64,
	concurrency int,
) error {
	// Create API client once
	apiClient, err := api.NewClient(config)

	if err != nil {
		return fmt.Errorf("failed to create API client: %w", err)
	}

	var (
		downloadedCount atomic.Int64
		errorsMu        sync.Mutex
		downloadErrors  []error
		wg              sync.WaitGroup
		p               *tea.Program
	)

	// Use Bubble Tea progress bar in interactive mode
	if config.GetInteractive() {
		prog := progress.New(progress.WithGradient(
			cli.Sky700.Hex(),
			cli.Sky300.Hex(),
		))

		model := exportProgressModel{
			progress:        prog,
			rangeCount:      rangeCount,
			downloadedCount: &downloadedCount,
		}

		p = tea.NewProgram(model)

		go func() {
			if _, err := p.Run(); err != nil {
				slog.Error("failed to run progress bar", "error", err)
			}
		}()
	} else {
		_, err := fmt.Fprintf(cmd.OutOrStdout(), "Downloading %d ranges with concurrency=%d...\n", rangeCount, concurrency)

		if err != nil {
			return fmt.Errorf("failed to print download message: %w", err)
		}
	}

	// Create worker pool with range indices
	jobQueue := make(chan int64, concurrency*2)

	// Start workers
	for w := range concurrency {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for rangeNumber := range jobQueue {
				// Download range
				rangePath := fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export/%s/ranges/%d",
					databaseName,
					branchName,
					exportID,
					rangeNumber,
				)

				// Use raw HTTP client
				httpClient := &http.Client{
					Timeout: 5 * time.Minute, // Ranges can be large
				}

				url := fmt.Sprintf("%s%s", apiClient.BaseURL, rangePath)
				req, err := http.NewRequest("GET", url, nil)

				if err != nil {
					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to create request for range %d: %w", rangeNumber, err))
					errorsMu.Unlock()

					continue
				}

				// Set required headers
				req.Header.Set("Content-Type", "application/json")

				// Add authentication headers
				if err := applyClientAuth(apiClient, req, "GET", rangePath, nil); err != nil {
					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to add auth headers for range %d: %w", rangeNumber, err))
					errorsMu.Unlock()

					continue
				}

				resp, err := httpClient.Do(req)

				if err != nil {
					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to download range %d: %w", rangeNumber, err))
					errorsMu.Unlock()

					continue
				}

				if resp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(resp.Body)

					if err != nil {
						if err := resp.Body.Close(); err != nil {
							slog.Error("Failed to close response body", "error", err)
						}

						errorsMu.Lock()
						downloadErrors = append(downloadErrors, fmt.Errorf("failed to read response body for range %d: %w", rangeNumber, err))
						errorsMu.Unlock()

						continue
					}

					if err := resp.Body.Close(); err != nil {
						slog.Error("Failed to close response body", "error", err)
					}

					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to download range %d with status %d: %s", rangeNumber, resp.StatusCode, string(body)))
					errorsMu.Unlock()

					continue
				}

				// Save range to temporary file
				rangeFilename := fmt.Sprintf("%s_%010d", baseFilename, rangeNumber)
				rangeFilePath := filepath.Join(tempDir, rangeFilename)

				rangeFile, err := os.Create(rangeFilePath)

				if err != nil {
					if err := resp.Body.Close(); err != nil {
						slog.Error("Failed to close response body", "error", err)
					}

					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to create range file %d: %w", rangeNumber, err))
					errorsMu.Unlock()

					continue
				}

				_, err = io.Copy(rangeFile, resp.Body)

				if err := resp.Body.Close(); err != nil {
					slog.Error("Failed to close response body", "error", err)
				}

				if err := rangeFile.Close(); err != nil {
					slog.Error("Failed to close range file", "error", err)
				}

				if err != nil {
					errorsMu.Lock()
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to write range file %d: %w", rangeNumber, err))
					errorsMu.Unlock()

					continue
				}

				// Update progress
				downloaded := downloadedCount.Add(1)

				if config.GetInteractive() && p != nil {
					p.Send(exportProgressMsg{downloaded: downloaded})
				} else {
					_, err = fmt.Fprintf(
						cmd.OutOrStdout(),
						"Downloaded range %d/%d (%.1f%%)\n",
						downloaded,
						rangeCount,
						float64(downloaded)/float64(rangeCount)*100,
					)

					if err != nil {
						// Don't fail on print errors, just log
						_, err := fmt.Fprintf(cmd.OutOrStderr(), "failed to print download progress: %v\n", err)

						if err != nil {
							slog.Error("Failed to print download progress", "error", err)
						}
					}
				}
			}
		}(w)
	}

	// Send range numbers to the queue (1-indexed)
	for i := int64(1); i <= rangeCount; i++ {
		jobQueue <- i
	}

	close(jobQueue)

	// Wait for all downloads to complete
	wg.Wait()

	// Stop the progress bar if it's running
	if config.GetInteractive() && p != nil {
		if len(downloadErrors) > 0 {
			p.Send(exportProgressErrMsg{err: downloadErrors[0]})
		} else {
			p.Send(exportProgressDoneMsg{})
		}

		// Brief delay to ensure the 100% state renders
		time.Sleep(200 * time.Millisecond)
		p.Quit()
		p.Wait()
	}

	// Check for errors
	if len(downloadErrors) > 0 {
		return fmt.Errorf("failed to download %d ranges: %v", len(downloadErrors), downloadErrors[0])
	}

	return nil
}

// Merge all range files into a single SQLite database file
func mergeRanges(tempDir string, baseFilename string, outputPath string, rangeCount int64) error {
	// Create the output file
	outFile, err := os.Create(outputPath)

	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	defer func() {
		if err := outFile.Close(); err != nil {
			slog.Error("Failed to close output file", "error", err)
		}
	}()

	// Concatenate all range files in order
	for i := int64(1); i <= rangeCount; i++ {
		rangeFilename := fmt.Sprintf("%s_%010d", baseFilename, i)
		rangeFilePath := filepath.Join(tempDir, rangeFilename)

		rangeFile, err := os.Open(rangeFilePath)

		if err != nil {
			return fmt.Errorf("failed to open range file %d: %w", i, err)
		}

		_, err = io.Copy(outFile, rangeFile)

		if err := rangeFile.Close(); err != nil {
			slog.Error("Failed to close range file", "error", err)
		}

		if err != nil {
			return fmt.Errorf("failed to copy range file %d: %w", i, err)
		}

		// Delete the temporary range file
		if err := os.Remove(rangeFilePath); err != nil {
			// Non-fatal error, just log it
			fmt.Printf("warning: failed to delete temporary file %s: %v\n", rangeFilePath, err)
		}
	}

	return nil
}

// applyClientAuth applies authentication to an HTTP request using the API client's configuration
func applyClientAuth(client *api.Client, req *http.Request, method string, path string, body []byte) error {
	host := client.BaseURL.Hostname()

	if client.BaseURL.Port() != "" {
		host = fmt.Sprintf("%s:%s", client.BaseURL.Hostname(), client.BaseURL.Port())
	}

	if client.Config.GetAccessKeyId() != "" && client.Config.GetAccessKeySecret() != "" {
		// Access key authentication
		headers := map[string]string{
			"Host":            host,
			"Content-Type":    "application/json",
			"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		// Parse query parameters from the path
		parsedURL, err := neturl.Parse(path)
		if err != nil {
			return fmt.Errorf("failed to parse path: %w", err)
		}

		// Extract query parameters
		queryParams := make(map[string]string)
		for key, values := range parsedURL.Query() {
			if len(values) > 0 {
				queryParams[key] = values[0]
			}
		}

		// Sign the request using the auth package
		signature := auth.SignRequest(
			client.Config.GetAccessKeyId(),
			client.Config.GetAccessKeySecret(),
			method,
			parsedURL.Path,
			headers,
			body,
			queryParams,
		)

		req.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))
	} else if client.Config.GetToken() != "" {
		// Token authentication
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", client.Config.GetToken()))
	} else if client.Config.GetUsername() != "" && client.Config.GetPassword() != "" {
		// Basic authentication
		req.SetBasicAuth(client.Config.GetUsername(), client.Config.GetPassword())
	}

	return nil
}

// Progress bar Bubble Tea model for export
type exportProgressModel struct {
	progress        progress.Model
	rangeCount      int64
	downloadedCount *atomic.Int64
	err             error
	done            bool
}

type exportProgressMsg struct {
	downloaded int64
}

type exportProgressErrMsg struct {
	err error
}

type exportProgressDoneMsg struct{}

func (m exportProgressModel) Init() tea.Cmd {
	// Start the progress bar's animation
	return m.progress.Init()
}

func (m exportProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit

	case exportProgressMsg:
		percent := float64(msg.downloaded) / float64(m.rangeCount)
		cmd := m.progress.SetPercent(percent)

		return m, cmd

	case exportProgressErrMsg:
		m.err = msg.err
		m.done = true

		return m, tea.Quit

	case exportProgressDoneMsg:
		m.done = true

		// Force progress to 100% before quitting
		percent := float64(m.downloadedCount.Load()) / float64(m.rangeCount)
		cmd := m.progress.SetPercent(percent)

		return m, tea.Sequence(cmd, tea.Quit)

	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		m.progress = progressModel.(progress.Model)

		return m, cmd

	case tea.WindowSizeMsg:
		m.progress.Width = min(msg.Width-4, 80)

		return m, nil

	default:
		return m, nil
	}
}

func (m exportProgressModel) View() string {
	if m.err != nil {
		return fmt.Sprintf("\nError: %v\n", m.err)
	}

	downloaded := m.downloadedCount.Load()
	percent := float64(downloaded) / float64(m.rangeCount)
	percentDisplay := percent * 100

	var progressBar string

	if m.done {
		// Use ViewAs for immediate 100% display without animation
		progressBar = m.progress.ViewAs(percent)
	} else {
		progressBar = m.progress.View()
	}

	return fmt.Sprintf(
		"\n  %s\n\n  Downloading ranges: %d/%d (%.1f%%)\n\n",
		progressBar,
		downloaded,
		m.rangeCount,
		percentDisplay,
	)
}
