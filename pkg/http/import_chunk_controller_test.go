package http_test

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestImportChunkControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("UploadChunk", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			// Create import first
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_chunk_db",
				"branchName":   "main",
				"chunkCount":   2,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Create a small chunk of data (1KB for testing)
			chunkData := make([]byte, 1024)

			for i := range chunkData {
				chunkData[i] = byte(i % 256)
			}

			encodedChunk := base64.StdEncoding.EncodeToString(chunkData)

			// Upload chunk
			uploadResp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/imports/%v/chunks", importID),
				"POST",
				map[string]any{
					"chunkData":  encodedChunk,
					"chunkIndex": 0,
				},
			)

			if err != nil {
				t.Fatalf("failed to upload chunk: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, uploadResp)
			}

			uploadData, ok := uploadResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in upload response")
			}

			if _, ok := uploadData["status"]; !ok {
				t.Error("expected status in response")
			}

			if _, ok := uploadData["chunkIndex"]; !ok {
				t.Error("expected chunkIndex in response")
			}
		})

		t.Run("InvalidBase64", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			// Create import first
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_invalid_base64_db",
				"branchName":   "main",
				"chunkCount":   1,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Upload chunk with invalid base64
			uploadResp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/imports/%v/chunks", importID),
				"POST",
				map[string]any{
					"chunkData":  "not-valid-base64!@#$",
					"chunkIndex": 0,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 400 {
				t.Fatalf("expected status code 400, got %d: %v", statusCode, uploadResp)
			}
		})

		t.Run("ImportNotFound", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeImport},
			}})

			chunkData := make([]byte, 1024)
			encodedChunk := base64.StdEncoding.EncodeToString(chunkData)

			resp, statusCode, err := client.Send(
				"/v1/imports/99999/chunks",
				"POST",
				map[string]any{
					"chunkData":  encodedChunk,
					"chunkIndex": 0,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("expected status code 404, got %d: %v", statusCode, resp)
			}
		})

		t.Run("InvalidChunkIndex", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			// Create import with 2 chunks
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_invalid_index_db",
				"branchName":   "main",
				"chunkCount":   2,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]
			chunkData := make([]byte, 1024)
			encodedChunk := base64.StdEncoding.EncodeToString(chunkData)

			// Try to upload chunk with index out of range
			uploadResp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/imports/%v/chunks", importID),
				"POST",
				map[string]any{
					"chunkData":  encodedChunk,
					"chunkIndex": 5,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 400 {
				t.Fatalf("expected status code 400, got %d: %v", statusCode, uploadResp)
			}
		})

		t.Run("UploadMultipleChunks", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			// Create import with 3 chunks
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_multiple_chunks_db",
				"branchName":   "main",
				"chunkCount":   3,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Upload 3 chunks
			for i := range 3 {
				chunkData := make([]byte, 1024)

				for j := range chunkData {
					chunkData[j] = byte((i*256 + j) % 256)
				}

				encodedChunk := base64.StdEncoding.EncodeToString(chunkData)

				uploadResp, statusCode, err := client.Send(
					fmt.Sprintf("/v1/imports/%v/chunks", importID),
					"POST",
					map[string]any{
						"chunkData":  encodedChunk,
						"chunkIndex": i,
					},
				)

				if err != nil {
					t.Fatalf("failed to upload chunk %d: %v", i, err)
				}

				if statusCode != 201 {
					t.Fatalf("expected status code 201 for chunk %d, got %d: %v", i, statusCode, uploadResp)
				}
			}

			// Verify all chunks uploaded
			getResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "GET", nil)

			if err != nil {
				t.Fatalf("failed to get import: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d: %v", statusCode, getResp)
			}

			getData, ok := getResp["data"].(map[string]any)

			if !ok {
				t.Fatal("expected data object in get response")
			}

			if getData["status"] != "completed" {
				t.Errorf("expected status 'completed', got %v", getData["status"])
			}

			uploadedChunks, ok := getData["uploadedChunks"].(float64)

			if !ok {
				t.Fatal("expected uploadedChunks to be a number")
			}

			if uploadedChunks != 3 {
				t.Errorf("expected 3 uploaded chunks, got %v", uploadedChunks)
			}

			missingChunks, ok := getData["missingChunks"].([]any)

			if !ok {
				// missingChunks might be null instead of empty array
				if getData["missingChunks"] != nil {
					t.Fatalf("expected missingChunks to be an array or null, got %T", getData["missingChunks"])
				}
			} else if len(missingChunks) != 0 {
				t.Errorf("expected 0 missing chunks, got %d", len(missingChunks))
			}
		})
	})
}
