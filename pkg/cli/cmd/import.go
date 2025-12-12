package cmd

import (
	"crypto/sha256"
	"encoding/base64"
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

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

const (
	chunkSize = 16 * 1024 * 1024 // 16MB chunks
)

type chunkJob struct {
	index    int64
	data     []byte
	checksum string
}

func NewImportCmd(config *config.CLIConfiguration) *cobra.Command {
	var concurrency int

	cmd := &cobra.Command{
		Use:   "import <file> <database>[/<branch>]",
		Args:  cobra.ExactArgs(2),
		Short: "Import a SQLite database file",
		Long:  "Import a SQLite database file by uploading it to the specified database and branch.",
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			databasePath := args[1]

			// Parse database/branch from path
			parts := strings.Split(databasePath, "/")

			if len(parts) < 1 || len(parts) > 2 {
				return errors.New("database path must be in format: database or database/branch")
			}

			databaseName := parts[0]
			branchName := "main"

			if len(parts) == 2 {
				branchName = parts[1]
			}

			// Check if file exists
			fileInfo, err := os.Stat(filePath)

			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("file not found: %s", filePath)
				}

				return fmt.Errorf("failed to access file: %w", err)
			}

			if fileInfo.IsDir() {
				return fmt.Errorf("path is a directory, not a file: %s", filePath)
			}

			// Calculate number of chunks
			fileSize := fileInfo.Size()
			chunkCount := (fileSize + chunkSize - 1) / chunkSize

			_, err = fmt.Fprintf(
				cmd.OutOrStdout(),
				"Importing %s (%d bytes) to %s/%s in %d chunks...\n",
				filepath.Base(filePath),
				fileSize,
				databaseName,
				branchName,
				chunkCount,
			)

			if err != nil {
				return fmt.Errorf("failed to print import message: %w", err)
			}

			// Create the import
			createData := map[string]any{
				"databaseName": databaseName,
				"branchName":   branchName,
				"chunkCount":   chunkCount,
			}

			res, apiErrors, err := api.Post(config, "/v1/imports", createData)

			if err != nil {
				return err
			}

			if len(apiErrors) > 0 {
				return apiErrors.Error()
			}

			// Extract import ID
			dataMap, ok := res["data"].(map[string]any)

			if !ok {
				return errors.New("invalid response format")
			}

			importIDFloat, ok := dataMap["importId"].(float64)

			if !ok {
				return errors.New("import ID not found in response")
			}

			importID := int64(importIDFloat)

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Import created with ID: %d\n", importID)

			if err != nil {
				return fmt.Errorf("failed to print import ID message: %w", err)
			}

			// Read and prepare all chunks first
			file, err := os.Open(filePath)

			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}

			defer func() {
				err := file.Close()
				if err != nil {
					_, err := fmt.Fprintf(cmd.OutOrStderr(), "failed to close file: %v\n", err)

					slog.Error("failed to close file", "error", err)
				}
			}()

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Reading file and preparing chunks...\n")

			if err != nil {
				return fmt.Errorf("failed to print reading message: %w", err)
			}

			chunks := make([]chunkJob, 0, chunkCount)
			buffer := make([]byte, chunkSize)

			for i := range chunkCount {
				// Read chunk
				n, err := io.ReadFull(file, buffer)

				if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
					return fmt.Errorf("failed to read chunk %d: %w", i, err)
				}

				// Copy the bytes actually read
				chunkData := make([]byte, n)
				copy(chunkData, buffer[:n])

				// Calculate checksum
				hash := sha256.Sum256(chunkData)
				checksum := fmt.Sprintf("%x", hash)

				chunks = append(chunks, chunkJob{
					index:    i,
					data:     chunkData,
					checksum: checksum,
				})
			}

			// Upload chunks concurrently
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Uploading %d chunks with concurrency=%d...\n", chunkCount, concurrency)

			if err != nil {
				return fmt.Errorf("failed to print upload message: %w", err)
			}

			var (
				uploadedCount atomic.Int64
				errorsMu      sync.Mutex
				uploadErrors  []error
				wg            sync.WaitGroup
			)

			// Create worker pool
			jobQueue := make(chan chunkJob, len(chunks))

			// Start workers
			for w := 0; w < concurrency; w++ {
				wg.Add(1)

				go func(workerID int) {
					defer wg.Done()

					for job := range jobQueue {
						// Encode to base64
						encodedData := base64.StdEncoding.EncodeToString(job.data)

						// Upload chunk
						chunkPayload := map[string]any{
							"chunkData":  encodedData,
							"chunkIndex": job.index,
							"checksum":   job.checksum,
						}

						_, apiErrors, err := api.Post(config, fmt.Sprintf("/v1/imports/%d/chunks", importID), chunkPayload)

						if err != nil {
							errorsMu.Lock()
							uploadErrors = append(uploadErrors, fmt.Errorf("failed to upload chunk %d: %w", job.index, err))
							errorsMu.Unlock()

							continue
						}

						if len(apiErrors) > 0 {
							errorsMu.Lock()
							uploadErrors = append(uploadErrors, fmt.Errorf("failed to upload chunk %d: %w", job.index, apiErrors.Error()))
							errorsMu.Unlock()

							continue
						}

						// Update progress
						uploaded := uploadedCount.Add(1)

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
				}(w)
			}

			// Send all jobs to the queue
			for _, chunk := range chunks {
				jobQueue <- chunk
			}

			close(jobQueue)

			// Wait for all uploads to complete
			wg.Wait()

			// Check for errors
			if len(uploadErrors) > 0 {
				return fmt.Errorf("failed to upload %d chunks: %v", len(uploadErrors), uploadErrors[0])
			}

			// Wait for import to complete
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Waiting for import to complete...\n")

			if err != nil {
				return fmt.Errorf("failed to print waiting message: %w", err)
			}

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
