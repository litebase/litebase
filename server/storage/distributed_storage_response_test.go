package storage_test

import (
	internalStorage "litebase/internal/storage"
	"litebase/server/storage"
	"testing"
)

func TestEncodedAndDecodeDistributedFileSystemResponse(t *testing.T) {
	dfsResponse := storage.DistributedFileSystemResponse{
		BytesProcessed: 20,
		Command:        storage.ConnectionStorageCommand,
		Data:           []byte("data"),
		Entries:        []internalStorage.DirEntry{},
		Error:          "error",
		FileInfo:       storage.StaticFileInfo{},
		Offset:         10,
		Path:           "path",
	}

	encodedResponse := dfsResponse.Encode()

	if len(encodedResponse) == 0 {
		t.Error("Expected response to be encoded")
	}

	newDfsResponse := storage.DistributedFileSystemResponse{}

	decodedResponse := storage.DecodeDistributedFileSystemResponse(newDfsResponse, encodedResponse)

	if decodedResponse.Command != storage.ConnectionStorageCommand {
		t.Error("Expected command to be ConnectionStorageCommand")
	}

	if decodedResponse.BytesProcessed != 20 {
		t.Error("Expected bytes processed to be 20")
	}

	if string(decodedResponse.Data) != "data" {
		t.Error("Expected data to be data")
	}

	if decodedResponse.Error != "error" {
		t.Error("Expected error to be error")
	}

	if decodedResponse.Offset != 10 {
		t.Error("Expected offset to be 10")
	}

	if decodedResponse.Path != "path" {
		t.Error("Expected path to be path")
	}

	if len(decodedResponse.Entries) != 0 {
		t.Error("Expected entries to be empty")
	}

	if decodedResponse.FileInfo != (storage.StaticFileInfo{}) {
		t.Error("Expected file info to be empty")
	}
}

func TestEncodedDistributedFileSystemResponseIsEmpty(t *testing.T) {
	dfsResponse := storage.DistributedFileSystemResponse{}

	if !dfsResponse.IsEmpty() {
		t.Error("Expected response to be empty")
	}
}

func TestEncodedDistributedFileSystemResponseReset(t *testing.T) {
	dfsResponse := storage.DistributedFileSystemResponse{
		BytesProcessed: 20,
		Command:        storage.ConnectionStorageCommand,
		Data:           []byte("data"),
		Entries:        []internalStorage.DirEntry{},
		Error:          "error",
		FileInfo:       storage.StaticFileInfo{},
		Offset:         10,
		Path:           "path",
	}

	dfsResponse = dfsResponse.Reset()

	if !dfsResponse.IsEmpty() {
		t.Error("Expected response to be empty")
	}
}
