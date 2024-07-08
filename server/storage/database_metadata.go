package storage

import (
	"encoding/json"
	"fmt"
	"litebase/server/file"
)

type DatabaseMetadata struct {
	BranchUuid   string `json:"branch_uuid"`
	DatabaseUuid string `json:"database_uuid"`
	PageCount    int64  `json:"page_count"`
}

func NewDatabaseMetadata(databaseUuid, branchUuid string) *DatabaseMetadata {
	metadata := &DatabaseMetadata{
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
	}

	err := metadata.Load()

	if err != nil {
		metadata.PageCount = 0
	}

	return metadata
}

// Load the database metadata
func (d *DatabaseMetadata) Load() error {
	data, err := FS().ReadFile(d.Path())

	if err != nil {
		return err
	}

	err = json.Unmarshal(data, d)

	if err != nil {
		return err
	}

	return nil
}

func (d *DatabaseMetadata) Path() string {
	return fmt.Sprintf("%s/%s.json", file.GetDatabaseFileDir(d.DatabaseUuid, d.BranchUuid), "_metadata")
}

// Save the database meta data
func (d *DatabaseMetadata) Save() error {
	data, err := json.Marshal(d)

	if err != nil {
		return err
	}

	return FS().WriteFile(d.Path(), data, 0644)
}

// Increment the page count
func (d *DatabaseMetadata) SetPageCount(count int64) error {
	d.PageCount = count

	return d.Save()
}
