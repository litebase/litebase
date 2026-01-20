package cluster

// NodeDatabaseManager interface to avoid circular dependencies with database package
type NodeDatabaseManager interface {
	Get(databaseID string) (NodeDatabase, error)
	GetSystemDatabase() NodeSystemDatabase
}

// NodeDatabase interface to represent a database
type NodeDatabase interface {
	BranchByID(branchID string) (NodeBranch, error)
}

// NodeBranch interface to represent a branch
type NodeBranch interface {
	GetBranchSettings() (NodeBranchSettings, error)
	SetSettings(settings NodeBranchSettings)
}

// NodeBranchSettings interface to represent branch settings
type NodeBranchSettings interface{}

// NodeSystemDatabase interface to interact with system database
type NodeSystemDatabase interface {
	OnMigrationsUpdated()
}
