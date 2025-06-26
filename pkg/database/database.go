package database

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Database struct {
	DatabaseManager   *DatabaseManager `json:"-"`
	Name              string           `json:"name"`
	Branches          []*Branch        `json:"branches"`
	Id                string           `json:"id"`
	PrimaryBranchId   string           `json:"primary_branch_id"`
	PrimaryBranchName string           `json:"primary_branch_name"`
	Settings          DatabaseSettings `json:"settings"`
	CreatedAt         time.Time        `json:"created_at"`
	UpdatedAt         time.Time        `json:"updated_at"`
}

func Directory() string {
	return "_databases/"
}

func TmpDirectory() string {
	return "_databases/"
}

func (database *Database) HasBranch(branchId string) bool {
	if database.Id == SystemDatabaseId && branchId == SystemDatabaseBranchId {
		return true
	}

	// TODO: This needs to be an actualy lookup on the system database
	for _, branch := range database.Branches {
		if branch.Id == branchId {
			return true
		}
	}

	return false
}

func (database *Database) Key(branchId string) string {
	var branch *Branch

	for _, b := range database.Branches {
		if b.Id == branchId {
			branch = b
			break
		}
	}

	return branch.Key
}

func (database *Database) save() error {
	err := database.DatabaseManager.Cluster.ObjectFS().MkdirAll(fmt.Sprintf("%s%s", Directory(), database.Id), 0750)

	if err != nil && !os.IsExist(err) {
		return err
	}

	jsonData, err := json.Marshal(database)

	if err != nil {
		return err
	}

	createError := database.DatabaseManager.Cluster.ObjectFS().WriteFile(fmt.Sprintf("%s%s/settings.json", Directory(), database.Id), jsonData, 0600)

	err = database.DatabaseManager.SecretsManager.StoreDatabaseKey(
		database.Key(database.PrimaryBranchId),
		database.Id,
		database.PrimaryBranchId,
	)

	if err != nil {
		return err
	}

	return createError
}

func (database *Database) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":              database.Name,
		"branches":          database.Branches,
		"id":                database.Id,
		"primary_branch_id": database.PrimaryBranchId,
		"settings":          database.Settings,
		"url":               database.Url(database.PrimaryBranchId),
		"created_at":        database.CreatedAt,
		"updated_at":        database.UpdatedAt,
	})
}

func (database *Database) BranchDirectory(branchId string) string {
	return fmt.Sprintf("%s%s/%s", Directory(), database.Id, branchId)
}

func (database *Database) Url(branchId string) string {
	port := ""

	if database.DatabaseManager.Cluster.Config.Port != "80" {
		port = fmt.Sprintf(":%s", database.DatabaseManager.Cluster.Config.Port)
	}

	return fmt.Sprintf(
		"http://%s%s/%s",
		database.DatabaseManager.Cluster.Config.HostName,
		port,
		database.Key(branchId),
	)
}
