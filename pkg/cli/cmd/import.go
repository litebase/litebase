package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/internal/utils/lock"
	"github.com/litebase/litebase/pkg/cli"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/spf13/cobra"
)

const (
	chunkSize = 16 * 1024 * 1024 // 16MB chunks
)

type ImportResponse struct {
	ImportID     int64     `json:"importId"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	ChunkCount   int64     `json:"chunkCount"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
}

func NewImportCmd(config *config.CLIConfiguration) *cobra.Command {
	var concurrency int

	cmd := &cobra.Command{
		Use:   "import <file> <database>[/<branch>]",
		Args:  cobra.ExactArgs(2),
		Short: "Import a SQLite database file",
		Long:  "Import a SQLite database file to a new database and branch.",
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, databaseName, branchName, err := parseImportArguments(args)

			if err != nil {
				return fmt.Errorf("failed to parse arguments: %w", err)
			}

			// Check if file exists
			fileInfo, err := parseFileInfo(filePath)

			if err != nil {
				return fmt.Errorf("file access failed: %w", err)
			}

			// Validate the SQLite file
			if err := validateSQLiteFile(filePath); err != nil {
				return fmt.Errorf("file validation failed: %w", err)
			}

			// Calculate number of chunks
			fileSize := fileInfo.Size()
			chunkCount := (fileSize + chunkSize - 1) / chunkSize

			chunkWord := "chunks"

			if chunkCount == 1 {
				chunkWord = "chunk"
			}

			_, err = fmt.Fprintf(
				cmd.OutOrStdout(),
				"Importing %s (%d bytes) to %s/%s in %d %s...\n",
				filepath.Base(filePath),
				fileSize,
				databaseName,
				branchName,
				chunkCount,
				chunkWord,
			)

			if err != nil {
				return fmt.Errorf("failed to print import message: %w", err)
			}

			// Create the import
			importResponse, err := createImport(config, databaseName, branchName, int(chunkCount))

			if err != nil {
				return fmt.Errorf("failed to create import: %w", err)
			}

			importID := importResponse.ImportID

			// Upload chunks concurrently
			if err := uploadChunksConcurrently(cmd, config, importID, filePath, chunkCount, concurrency); err != nil {
				return fmt.Errorf("failed to upload chunks: %w", err)
			}

			// Wait for import to complete
			if err := checkImportStatus(config, importID); err != nil {
				return fmt.Errorf("failed to wait for import to complete: %w", err)
			}

			message := fmt.Sprintf(
				"Successfully imported %s to %s/%s",
				filepath.Base(filePath),
				databaseName,
				branchName,
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

	cmd.Flags().IntVarP(&concurrency, "concurrency", "n", 3, "Number of chunks to upload concurrently")

	return cmd
}

func checkImportStatus(config *config.CLIConfiguration, importID int64) error {
	for {
		time.Sleep(1 * time.Second)

		statusRes, err := api.Get(config, fmt.Sprintf("/v1/imports/%d", importID))

		if err != nil {
			return fmt.Errorf("failed to check import status: %w", err)
		}

		statusData, ok := statusRes["data"].(map[string]any)

		if !ok {
			return errors.New("invalid status response format")
		}

		status, ok := statusData["status"].(string)

		if !ok {
			return errors.New("status not found in response")
		}

		if status == "completed" {
			break
		} else if status == "failed" {
			return errors.New("import failed")
		}
	}

	return nil
}

// Create a new import with the Litebase API and return the response.
func createImport(config *config.CLIConfiguration, databaseName string, branchName string, chunkCount int) (*ImportResponse, error) {
	createData := map[string]any{
		"databaseName": databaseName,
		"branchName":   branchName,
		"chunkCount":   chunkCount,
	}

	res, apiErrors, err := api.Post(config, "/v1/imports", createData)

	if err != nil {
		return nil, err
	}

	if len(apiErrors) > 0 {
		return nil, apiErrors.Error()
	}

	// Extract data from response
	dataMap, ok := res["data"].(map[string]any)

	if !ok {
		return nil, errors.New("invalid response format")
	}

	// Extract import ID
	importIDFloat, ok := dataMap["importId"].(float64)

	if !ok {
		return nil, errors.New("import ID not found in response")
	}

	// Build response
	response := &ImportResponse{
		ImportID:     int64(importIDFloat),
		DatabaseName: databaseName,
		BranchName:   branchName,
		ChunkCount:   int64(chunkCount),
	}

	// Extract optional fields if present
	if status, ok := dataMap["status"].(string); ok {
		response.Status = status
	}

	if databaseID, ok := dataMap["databaseId"].(string); ok {
		response.DatabaseID = databaseID
	}

	return response, nil
}

// Check if the file exists and return its metadata.
func parseFileInfo(filePath string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(filePath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", filePath)
		}

		return nil, fmt.Errorf("failed to access file: %w", err)
	}

	if fileInfo.IsDir() {
		return nil, fmt.Errorf("path is a directory, not a file: %s", filePath)
	}

	return fileInfo, nil
}

// Parse the import command arguments into a file path, database name, and branch name.
func parseImportArguments(args []string) (string, string, string, error) {
	if len(args) != 2 {
		return "", "", "", fmt.Errorf("invalid number of arguments: %d", len(args))
	}

	filePath := args[0]
	databasePath := args[1]

	// Parse database/branch from path
	parts := strings.Split(databasePath, "/")

	if len(parts) < 1 || len(parts) > 2 {
		return "", "", "", errors.New("database path must be in format: database or database/branch")
	}

	databaseName := parts[0]
	branchName := "main"

	if len(parts) == 2 {
		branchName = parts[1]
	}

	return filePath, databaseName, branchName, nil
}

// Upload chunks concurrently
func uploadChunksConcurrently(cmd *cobra.Command, config *config.CLIConfiguration, importID int64, filePath string, chunkCount int64, concurrency int) error {
	var (
		uploadedCount atomic.Int64
		errorsMu      sync.Mutex
		uploadErrors  []error
		wg            sync.WaitGroup
		fileMu        sync.Mutex
		p             *tea.Program
	)

	// Use Bubble Tea progress bar in interactive mode
	if config.GetInteractive() {
		prog := progress.New(progress.WithGradient(
			cli.Sky700.Hex(),
			cli.Sky300.Hex(),
		))

		model := progressModel{
			progress:      prog,
			chunkCount:    chunkCount,
			uploadedCount: &uploadedCount,
		}

		p = tea.NewProgram(model)

		go func() {
			if _, err := p.Run(); err != nil {
				slog.Error("failed to run progress bar", "error", err)
			}
		}()
	} else {
		_, err := fmt.Fprintf(cmd.OutOrStdout(), "Uploading %d chunks with concurrency=%d...\n", chunkCount, concurrency)

		if err != nil {
			return fmt.Errorf("failed to print upload message: %w", err)
		}
	}

	// Open file once for all workers to share
	file, err := os.Open(filePath)

	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	defer func() {
		err := file.Close()

		if err != nil {
			slog.Error("failed to close file", "error", err)
		}
	}()

	// Create worker pool with chunk indices
	jobQueue := make(chan int64, concurrency*2)

	// Start workers
	for w := 0; w < concurrency; w++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			buffer := make([]byte, chunkSize)

			for chunkIndex := range jobQueue {
				// Read chunk from file (with synchronization)
				fileMu.Lock()
				offset := chunkIndex * chunkSize
				_, err := file.Seek(offset, 0)

				if err != nil {
					fileMu.Unlock()
					errorsMu.Lock()
					uploadErrors = append(uploadErrors, fmt.Errorf("failed to seek to chunk %d: %w", chunkIndex, err))
					errorsMu.Unlock()

					continue
				}

				n, err := io.ReadFull(file, buffer)

				if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
					fileMu.Unlock()
					errorsMu.Lock()
					uploadErrors = append(uploadErrors, fmt.Errorf("failed to read chunk %d: %w", chunkIndex, err))
					errorsMu.Unlock()

					continue
				}

				fileMu.Unlock()

				// Copy the bytes actually read
				chunkData := make([]byte, n)
				copy(chunkData, buffer[:n])

				// Calculate checksum
				hash := sha256.Sum256(chunkData)
				checksum := fmt.Sprintf("%x", hash)

				// Encode to base64
				encodedData := base64.StdEncoding.EncodeToString(chunkData)

				// Upload chunk
				chunkPayload := map[string]any{
					"chunkData":  encodedData,
					"chunkIndex": chunkIndex,
					"checksum":   checksum,
				}

				_, apiErrors, err := api.Post(config, fmt.Sprintf("/v1/imports/%d/chunks", importID), chunkPayload)

				if err != nil {
					errorsMu.Lock()
					uploadErrors = append(uploadErrors, fmt.Errorf("failed to upload chunk %d: %w", chunkIndex, err))
					errorsMu.Unlock()

					continue
				}

				if len(apiErrors) > 0 {
					errorsMu.Lock()
					uploadErrors = append(uploadErrors, fmt.Errorf("failed to upload chunk %d: %w", chunkIndex, apiErrors.Error()))
					errorsMu.Unlock()

					continue
				}

				// Update progress
				uploaded := uploadedCount.Add(1)

				if config.GetInteractive() && p != nil {
					p.Send(progressMsg{uploaded: uploaded})
				} else {
					_, err = fmt.Fprintf(
						cmd.OutOrStdout(),
						"Uploaded chunk %d/%d (%.1f%%)\n",
						uploaded,
						chunkCount,
						float64(uploaded)/float64(chunkCount)*100,
					)

					if err != nil {
						_, err := fmt.Fprintf(cmd.OutOrStderr(), "failed to print upload progress: %v\n", err)

						slog.Error("failed to print upload progress", "error", err)
					}
				}
			}
		}(w)
	}

	// Send chunk indices to the queue
	for i := range chunkCount {
		jobQueue <- i
	}

	close(jobQueue)

	// Wait for all uploads to complete
	wg.Wait()

	// Stop the progress bar if it's running
	if config.GetInteractive() && p != nil {
		if len(uploadErrors) > 0 {
			p.Send(progressErrMsg{err: uploadErrors[0]})
		} else {
			p.Send(progressDoneMsg{})
		}

		// Brief delay to ensure the 100% state renders
		time.Sleep(200 * time.Millisecond)
		p.Quit()
		p.Wait()
	}

	// Check for errors
	if len(uploadErrors) > 0 {
		return fmt.Errorf("failed to upload %d chunks: %v", len(uploadErrors), uploadErrors[0])
	}

	// Wait for import to complete
	if !config.GetInteractive() {
		_, err := fmt.Fprintf(cmd.OutOrStdout(), "Waiting for import to complete...\n")

		if err != nil {
			return fmt.Errorf("failed to print waiting message: %w", err)
		}
	}

	return nil
}

// Validate that the file is a valid SQLite v3 database with the correct
// configuration for import.
func validateSQLiteFile(filePath string) error {
	// First, read and validate the SQLite header
	file, err := os.Open(filePath)

	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	// Try to acquire a non-blocking exclusive lock to ensure file isn't being modified
	locked, err := lock.LockFile(file)

	if err != nil {
		err := file.Close()

		if err != nil {
			slog.Error("failed to close file after lock check", "error", err)
		}

		return fmt.Errorf("failed to check file lock: %w", err)
	}

	if !locked {
		err = file.Close()

		if err != nil {
			slog.Error("failed to close file after lock check", "error", err)
		}

		return errors.New("database file is currently locked by another process")
	}

	// Read the SQLite header (first 100 bytes contain all we need)
	header := make([]byte, 100)

	if _, err := io.ReadFull(file, header); err != nil {
		err := lock.UnlockFile(file)

		if err != nil {
			slog.Error("failed to unlock file after lock check", "error", err)
		}

		err = file.Close()

		if err != nil {
			slog.Error("failed to close file after reading header", "error", err)
		}

		return fmt.Errorf("failed to read database header: %w", err)
	}

	// Validate SQLite magic string "SQLite format 3\000"
	expectedMagic := []byte("SQLite format 3\x00")

	if !bytes.Equal(header[0:16], expectedMagic) {
		err = lock.UnlockFile(file)

		if err != nil {
			slog.Error("failed to unlock file after lock check", "error", err)
		}

		err = file.Close()

		if err != nil {
			slog.Error("failed to close file after reading header", "error", err)
		}

		return errors.New("file is not a valid SQLite database")
	}

	// Read page size from bytes 16-17 (big-endian)
	// Special case: if value is 1, actual page size is 65536
	pageSizeRaw := binary.BigEndian.Uint16(header[16:18])
	var pageSize uint32

	if pageSizeRaw == 1 {
		pageSize = 65536
	} else {
		pageSize = uint32(pageSizeRaw)
	}

	// Validate page size is 4096
	if pageSize != 4096 {
		err = lock.UnlockFile(file)

		if err != nil {
			slog.Error("failed to unlock file after lock check", "error", err)
		}

		err = file.Close()

		if err != nil {
			slog.Error("failed to close file after reading header", "error", err)
		}

		return fmt.Errorf("database page size must be 4096 bytes, got %d", pageSize)
	}

	// Close the file before opening with SQLite
	err = lock.UnlockFile(file)

	if err != nil {
		slog.Error("failed to unlock file after lock check", "error", err)
	}

	err = file.Close()

	if err != nil {
		slog.Error("failed to close file after reading header", "error", err)
	}

	// Open database connection to check journal mode and run integrity check
	ctx := context.Background()
	conn, err := sqlite3.Open(ctx, filePath, "", sqlite3.SQLITE_OPEN_READONLY)

	if err != nil {
		return fmt.Errorf("failed to open database for validation: %w", err)
	}

	defer func() {
		err := conn.Close()

		if err != nil {
			slog.Error("failed to close database connection", "error", err)
		}
	}()

	// Check journal mode
	journalModeResult := sqlite3.NewResult()

	stmt, _, err := conn.Prepare(ctx, "PRAGMA journal_mode")

	if err != nil {
		return fmt.Errorf("failed to check journal mode: %w", err)
	}

	defer func() {
		err := stmt.Finalize()

		if err != nil {
			slog.Error("failed to finalize statement", "error", err)
		}
	}()

	if err := stmt.Exec(journalModeResult); err != nil {
		return fmt.Errorf("failed to execute journal mode check: %w", err)
	}

	if len(journalModeResult.Rows) == 0 || len(journalModeResult.Rows[0]) == 0 {
		return errors.New("failed to determine journal mode")
	}

	journalMode := strings.ToLower(string(journalModeResult.Rows[0][0].ColumnValue))

	// If not in DELETE mode, check for journal files
	if journalMode != "delete" {
		// Check for WAL file
		walPath := filePath + "-wal"

		if _, err := os.Stat(walPath); err == nil {
			return fmt.Errorf("database is in %s mode and has a WAL file (%s), please checkpoint the database first", journalMode, walPath)
		}

		// Check for journal file
		journalPath := filePath + "-journal"

		if _, err := os.Stat(journalPath); err == nil {
			return fmt.Errorf("database is in %s mode and has a journal file (%s), please complete or rollback pending transactions", journalMode, journalPath)
		}
	}

	// Run integrity check
	integrityResult := sqlite3.NewResult()
	integrityStmt, _, err := conn.Prepare(ctx, "PRAGMA integrity_check")

	if err != nil {
		return fmt.Errorf("failed to prepare integrity check: %w", err)
	}

	defer func() {
		err := integrityStmt.Finalize()

		if err != nil {
			slog.Error("failed to finalize statement", "error", err)
		}
	}()

	if err := integrityStmt.Exec(integrityResult); err != nil {
		return fmt.Errorf("failed to execute integrity check: %w", err)
	}

	if len(integrityResult.Rows) == 0 || len(integrityResult.Rows[0]) == 0 {
		return errors.New("integrity check returned no results")
	}

	integrityStatus := string(integrityResult.Rows[0][0].ColumnValue)

	if integrityStatus != "ok" {
		return fmt.Errorf("database integrity check failed: %s", integrityStatus)
	}

	return nil
}

// Progress bar Bubble Tea model
type progressModel struct {
	progress      progress.Model
	chunkCount    int64
	uploadedCount *atomic.Int64
	err           error
	done          bool
}

type progressMsg struct {
	uploaded int64
}

type progressErrMsg struct {
	err error
}

type progressDoneMsg struct{}

func (m progressModel) Init() tea.Cmd {
	// Start the progress bar's animation
	return m.progress.Init()
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit

	case progressMsg:
		percent := float64(msg.uploaded) / float64(m.chunkCount)
		cmd := m.progress.SetPercent(percent)

		return m, cmd

	case progressErrMsg:
		m.err = msg.err
		m.done = true
		return m, tea.Quit

	case progressDoneMsg:
		m.done = true

		// Force progress to 100% before quitting
		percent := float64(m.uploadedCount.Load()) / float64(m.chunkCount)
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

func (m progressModel) View() string {
	if m.err != nil {
		return fmt.Sprintf("\nError: %v\n", m.err)
	}

	uploaded := m.uploadedCount.Load()
	percent := float64(uploaded) / float64(m.chunkCount)
	percentDisplay := percent * 100

	var progressBar string

	if m.done {
		// Use ViewAs for immediate 100% display without animation
		progressBar = m.progress.ViewAs(percent)
	} else {
		progressBar = m.progress.View()
	}

	return fmt.Sprintf(
		"\n  %s\n\n  Uploading chunks: %d/%d (%.1f%%)\n\n",
		progressBar,
		uploaded,
		m.chunkCount,
		percentDisplay,
	)
}
