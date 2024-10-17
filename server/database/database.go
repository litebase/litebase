package database

import (
	"encoding/json"
	"fmt"
)

type Database struct {
	DatabaseManager   *DatabaseManager `json:"-"`
	Name              string           `json:"name"`
	Branches          []*Branch        `json:"branches"`
	Id                string           `json:"id"`
	PrimaryBranchId   string           `json:"primary_branch_id"`
	PrimaryBranchName string           `json:"primary_branch_name"`
	Settings          DatabaseSettings `json:"settings"`
}

func Directory() string {
	return "_databases/"
}

func TmpDirectory() string {
	return "_databases/"
}

func (database *Database) HasBranch(branchId string) bool {
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
	database.DatabaseManager.Cluster.ObjectFS().MkdirAll(fmt.Sprintf("%s%s", Directory(), database.Id), 0755)

	jsonData, err := json.Marshal(database)

	if err != nil {
		return err
	}

	createError := database.DatabaseManager.Cluster.ObjectFS().WriteFile(fmt.Sprintf("%s%s/settings.json", Directory(), database.Id), jsonData, 0666)

	database.DatabaseManager.SecretsManager.StoreDatabaseKey(
		database.Key(database.PrimaryBranchId),
		database.Id,
		database.PrimaryBranchId,
	)

	return createError
}

func (database *Database) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":              database.Name,
		"branches":          database.Branches,
		"id":                database.Id,
		"primary_branch_id": database.PrimaryBranchId,
		"settings":          database.Settings,
		"url":               database.Url(database.PrimaryBranchId),
	})
}

func (database *Database) BranchDirectory(branchId string) string {
	return fmt.Sprintf("%s%s/%s", Directory(), database.Id, branchId)
}

func (database *Database) Url(branchId string) string {
	return fmt.Sprintf(
		"http://%s.%s.%s.litebase.test:%s",
		database.Key(database.PrimaryBranchId),
		// TODO: Get the region
		"region",
		database.DatabaseManager.Cluster.Id,
		// TODO: Make optional for production
		database.DatabaseManager.Cluster.Config.Port,
	)
}
