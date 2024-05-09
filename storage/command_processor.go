package storage

import (
	"litebasedb/internal/storage"
)

const (
	READ     = "READ"
	WRITE    = "WRITE"
	DELETE   = "DELETE"
	TRUNCATE = "TRUNCATE"
)

type CommandProcessor struct {
}

func NewCommandProcessor() *CommandProcessor {
	return &CommandProcessor{}
}

func (cp *CommandProcessor) Run(request storage.StorageRequest) ([]byte, error) {
	switch request.Command {
	case READ:
		return read(request.DatabaseUuid, request.BranchUuid, request.Key, request.Page)
	case WRITE:
		err := write(request.DatabaseUuid, request.BranchUuid, request.Key, request.Data)

		return nil, err
	case DELETE:
		// Delete the data
	case TRUNCATE:
		// Truncate the data
	default:
		// Do nothing
	}

	return nil, nil
}
