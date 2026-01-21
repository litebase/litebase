package database

import (
	"github.com/litebase/litebase/pkg/cluster"
)

// DatabaseManagerAdapter adapts DatabaseManager to implement cluster.NodeDatabaseManager interface
type DatabaseManagerAdapter struct {
	databaseManager *DatabaseManager
}

// NewDatabaseManagerAdapter creates a new DatabaseManagerAdapter
func NewDatabaseManagerAdapter(databaseManager *DatabaseManager) *DatabaseManagerAdapter {
	return &DatabaseManagerAdapter{
		databaseManager: databaseManager,
	}
}

// Get implements cluster.NodeDatabaseManager
func (dma *DatabaseManagerAdapter) Get(databaseID string) (cluster.NodeDatabase, error) {
	db, err := dma.databaseManager.Get(databaseID)

	if err != nil {
		return nil, err
	}

	return &DatabaseAdapter{db: db}, nil
}

// GetSystemDatabase implements cluster.NodeDatabaseManager
func (dma *DatabaseManagerAdapter) GetSystemDatabase() cluster.NodeSystemDatabase {
	return dma.databaseManager.SystemDatabase()
}

// DatabaseAdapter adapts Database to implement cluster.NodeDatabase interface
type DatabaseAdapter struct {
	db *Database
}

// BranchByID implements cluster.NodeDatabase
func (da *DatabaseAdapter) BranchByID(branchID string) (cluster.NodeBranch, error) {
	branch, err := da.db.BranchByID(branchID)

	if err != nil {
		return nil, err
	}

	return &BranchAdapter{branch: branch}, nil
}

// BranchAdapter adapts Branch to implement cluster.NodeBranch interface
type BranchAdapter struct {
	branch *Branch
}

// GetBranchSettings implements cluster.NodeBranch
func (ba *BranchAdapter) GetBranchSettings() (cluster.NodeBranchSettings, error) {
	return ba.branch.GetBranchSettings()
}

// SetSettings implements cluster.NodeBranch
func (ba *BranchAdapter) SetSettings(settings cluster.NodeBranchSettings) {
	if branchSettings, ok := settings.(*DatabaseBranchSettings); ok {
		ba.branch.Settings = branchSettings
	}
}
