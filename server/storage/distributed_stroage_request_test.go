package storage_test

import (
	"litebase/server/storage"
	"testing"
)

func TestEncodedAndDecodeDistributedFileSystemRequest(t *testing.T) {
	dfsRequest := storage.DistributedFileSystemRequest{
		Command: storage.ConnectionStorageCommand,
		Data:    []byte("data"),
		Flag:    1,
		Length:  10,
		Offset:  20,
		OldPath: "oldPath",
		Path:    "path",
		Perm:    2,
		Size:    30,
		Whence:  3,
	}

	encodedRequest := dfsRequest.Encode()

	if len(encodedRequest) == 0 {
		t.Error("Expected request to be encoded")
	}

	newDfsRequest := storage.DistributedFileSystemRequest{}

	newDfsRequest, err := storage.DecodeDistributedFileSystemRequest(newDfsRequest, encodedRequest)

	if err != nil {
		t.Error("Expected request to be decoded")
	}

	if newDfsRequest.Command != storage.ConnectionStorageCommand {
		t.Error("Expected command to be ConnectionStorageCommand")
	}

	if string(newDfsRequest.Data) != "data" {
		t.Error("Expected data to be data")
	}

	if newDfsRequest.Flag != 1 {
		t.Error("Expected flag to be 1")
	}

	if newDfsRequest.Length != 10 {
		t.Error("Expected length to be 10")
	}

	if newDfsRequest.Offset != 20 {
		t.Error("Expected offset to be 20")
	}

	if newDfsRequest.OldPath != "oldPath" {
		t.Error("Expected old path to be oldPath")
	}

	if newDfsRequest.Path != "path" {
		t.Error("Expected path to be path")
	}

	if newDfsRequest.Perm != 2 {
		t.Error("Expected perm to be 2")
	}

	if newDfsRequest.Size != 30 {
		t.Error("Expected size to be 30")
	}

	if newDfsRequest.Whence != 3 {
		t.Error("Expected whence to be 3")
	}
}

func TestEncodedDistributedFileSystemRequestIsEmpty(t *testing.T) {
	dfsRequest := storage.DistributedFileSystemRequest{}

	if !dfsRequest.IsEmpty() {
		t.Error("Expected request to be empty")
	}
}

func TestEncodedDistributedFileSystemRequestReset(t *testing.T) {
	dfsRequest := storage.DistributedFileSystemRequest{
		Command: storage.ConnectionStorageCommand,
		Data:    []byte("data"),
		Flag:    1,
		Length:  10,
		Offset:  20,
		OldPath: "oldPath",
		Path:    "path",
		Perm:    2,
		Size:    30,
		Whence:  3,
	}

	dfsRequest = dfsRequest.Reset()

	if !dfsRequest.IsEmpty() {
		t.Error("Expected request to be empty")
	}
}
