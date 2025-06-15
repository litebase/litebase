package auth

import (
	"strings"
)

/*
Access Key permissions are defined by a list of resources and statements that
can be performed on those resources.

When an Access Key has a rule with the resource defined as "*", this indicates
that the access key rule is scoped to all resources. Rules can also be scoped to
a specific resource, such as a database, branch, or table.

| Scope                                | Example                                                                     |
|--------------------------------------|-----------------------------------------------------------------------------|
| All resources                        | `*`                                                                         |
| All database resources               | `database:*`                                                                |
| A specific database                  | `database:DATABASE_ID`                                                      |
| All resources of a specific database | `database:DATABASE_ID:*`                                                    |
| All branch resources of a database   | `database:DATABASE_ID:branch:*`                                             |
| A specific branch of a database      | `database:DATABASE_ID:branch:BRANCH_ID`                                     |
| All resources of a specific branch   | `database:DATABASE_ID:branch:BRANCH_ID:*`                                   |
| All table resources of a branch      | `database:DATABASE_ID:branch:BRANCH_ID:table:*`                             |
| A specific table of a branch         | `database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME`                    |
| All resources of a table             | `database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME:*`                  |
| All column resources of a table      | `database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME:column:*`           |
| A specific column of a table         | `database:DATABASE_ID:branch:BRANCH_ID:table:TABLE_NAME:column:COLUMN_NAME` |

When an Access Key has a rule with Actions defined as "*", this indicates that
the access key rule is scoped to all actions. Actions can also be scoped to
specific actions, such as `database:analyze`, `database:attach`,
`database:alter_table`, `database:create_index`, etc.

See the full list of DatabasePrivileges in `server/auth/database_privileges.go`.
*/
func (accessKey *AccessKey) authorizationKey(strs ...string) string {
	return strings.Join(strs, ":")
}

func (accessKey *AccessKey) authorizedForBranch(databaseId, branchId string, privilege Privilege) bool {
	// Any resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("*"), privilege) {
		return true
	}

	if Authorized(accessKey.Statements, accessKey.authorizationKey("database:*"), privilege) {
		return true
	}

	// Any resource of the database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "*"), privilege) {
		return true
	}

	// Any branch resource of the database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", "*"), privilege) {
		return true
	}

	// Any resource of the specific branch of the database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "*"), privilege) {
		return true
	}

	// A specific branch of a specific database
	return Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId), privilege)
}

func (accessKey *AccessKey) authorizedForColumn(databaseId, branchId, table, column string, privilege Privilege) bool {
	// Any resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("*"), privilege) {
		return true
	}

	// Any resource of the database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "*"), privilege) {
		return true
	}

	// Any resources of the branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "*"), privilege) {
		return true
	}

	// Any resource of the table
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", table, "*"), privilege) {
		return true
	}

	// Any column resource of the table
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", table, "column", "*"), privilege) {
		return true
	}

	// A specific column of a specific table of a specific database
	return Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", table, "column", column), privilege)
}

// Determine if an Access Key is authorized to perform an action on a database.
func (accessKey *AccessKey) authorizedForDatabase(databaseId string, privilege Privilege) bool {
	// Any resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("*"), privilege) {
		return true
	}

	// Any database resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", "*"), privilege) {
		return true
	}

	// A specific database
	return Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId), privilege)
}

// Determine if an Access Key is authorized to perform an action on a table.
func (accessKey *AccessKey) authorizedForTable(databaseId, branchId, table string, privilege Privilege) bool {
	// Any resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("*"), privilege) {
		return true
	}

	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", "*"), privilege) {
		return true
	}

	// Any resource of the specific database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "*"), privilege) {
		return true
	}

	// Any resource of the specific database and branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", "*"), privilege) {
		return true
	}

	// Any resource of the specific database and branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId), privilege) {
		return true
	}

	// Any resource of the specific database and branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "*"), privilege) {
		return true
	}

	// Any table resource of the specific branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", "*"), privilege) {
		return true
	}

	// A specific table of a specific database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", table), privilege) {
		return true
	}

	// Any resource of the specific table
	return Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "table", table, "*"), privilege)
}

// Determine if an Access Key is authorized to perform an action on a module.
func (accessKey *AccessKey) authorizedForVTable(databaseId, branchId, module, vtable string, privilege Privilege) bool {
	// Any resource
	if Authorized(accessKey.Statements, accessKey.authorizationKey("*"), privilege) {
		return true
	}

	// Any resource of the specific database
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "*"), privilege) {
		return true
	}

	// Any resource of the specific database and branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "*"), privilege) {
		return true
	}

	// Any module resource of the specific branch
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "module", "*"), privilege) {
		return true
	}

	// Any vtable resource of the specific module
	if Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "module", module, "vtable", "*"), privilege) {
		return true
	}

	// A specific vtable of a specific module of a specific database
	return Authorized(accessKey.Statements, accessKey.authorizationKey("database", databaseId, "branch", branchId, "module", module, "vtable", vtable), privilege)
}

// Determine if an Access Key is authorized to perform an action on a branch.
func (accessKey *AccessKey) CanAccessDatabase(databaseId, branchId string) error {
	if databaseId == "" || branchId == "" {
		return NewDatabaseAccessError()
	}

	for _, statement := range accessKey.Statements {
		if statement.Resource == "*" {
			return nil
		}

		if statement.Resource.HasPrefix(accessKey.authorizationKey(databaseId, branchId)) {
			return nil
		}
	}

	return NewDatabaseAccessError()
}

// Determine if an Access Key is authorized to perform an analyze on a database.
func (accessKey *AccessKey) CanAnalyze(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeAnalyze) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeAnalyze)
}

// Determine if an Access Key is authorized to perform an attach on a database.
func (accessKey *AccessKey) CanAttach(databaseId, branchId, database string) (bool, error) {
	return false, NewDatabasePrivilegeError(DatabasePrivilegeAttach)
}

// Determine if an Access Key is authorized to perform an alter on a database.
func (accessKey *AccessKey) CanAlterTable(databaseId, branchId, database, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeAlterTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeAlterTable)
}

// Determine if an Access Key is authorized to perform a create index on a database.
func (accessKey *AccessKey) CanCreateIndex(databaseId, branchId, tableName, indexName string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, tableName, DatabasePrivilegeCreateIndex) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateIndex)
}

// Determine if an Access Key is authorized to perform a create table on a database.
func (accessKey *AccessKey) CanCreateTable(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeCreateTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTable)
}

// Determine if an Access Key is authorized to perform a create temp table on a database.
func (accessKey *AccessKey) CanCreateTempTable(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeCreateTempTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTempTable)
}

// Determine if an Access Key is authorized to perform a create temp trigger on a database.
func (accessKey *AccessKey) CanCreateTempTrigger(databaseId, branchId, tableName, triggerName string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, tableName, DatabasePrivilegeCreateTempTrigger) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTempTrigger)
}

// Determine if an Access Key is authorized to perform a create temp view on a database.
func (accessKey *AccessKey) CanCreateTempView(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeCreateTempView) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTempView)
}

// Determine if an Access Key is authorized to perform a create trigger on a database.
func (accessKey *AccessKey) CanCreateTrigger(databaseId, branchId, tableName, triggerName string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, tableName, DatabasePrivilegeCreateTrigger) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTrigger)
}

// Determine if an Access Key is authorized to perform a create view on a database.
func (accessKey *AccessKey) CanCreateView(databaseId, branchId, view string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, view, DatabasePrivilegeCreateView) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateView)
}

// Determine if an Access Key is authorized to perform a create vtable on a database.
func (accessKey *AccessKey) CanCreateVTable(databaseId, branchId, moduleName, vtable string) (bool, error) {
	if accessKey.authorizedForVTable(databaseId, branchId, moduleName, vtable, DatabasePrivilegeCreateVTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateVTable)
}

// Determine if an Access Key is authorized to perform a delete on a database.
func (accessKey *AccessKey) CanDelete(databaseId, branchId string, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDelete) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDelete)
}

// Determine if an Access Key is authorized to perform a detach on a database.
func (accessKey *AccessKey) CanDetach(databaseId, branchId, database string) (bool, error) {
	return false, NewDatabasePrivilegeError(DatabasePrivilegeAttach)
}

// Determine if an Access Key is authorized to perform a drop index on a database.
func (accessKey *AccessKey) CanDropIndex(databaseId, branchId, table, index string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropIndex) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropIndex)
}

// Determine if an Access Key is authorized to perform a drop table on a database.
func (accessKey *AccessKey) CanDropTable(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTable)
}

// Determine if an Access Key is authorized to perform a drop trigger on a database.
func (accessKey *AccessKey) CanDropTrigger(databaseId, branchId, table, trigger string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropTrigger) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTrigger)
}

// Determine if an Access Key is authorized to perform a drop view on a database.
func (accessKey *AccessKey) CanDropView(databaseId, branchId, view string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, view, DatabasePrivilegeDropView) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropView)
}

// Determine if an Access Key is authorized to perform a function on a database.
func (accessKey *AccessKey) CanFunction(databaseId, branchId, function string) (bool, error) {
	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegeFunction) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeFunction)
}

// Determine if an Access Key is authorized to perform a function on a database.
func (accessKey *AccessKey) CanInsert(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeInsert) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeInsert)
}

// Determine if an Access Key is authorized to perform a pragma on a database.
func (accessKey *AccessKey) CanPragma(databaseId, branchId, pragma, value string) (bool, error) {
	var (
		pragmaAllowed bool
		ok            bool
	)

	if pragmaAllowed, ok = PragmaList[pragma]; !ok {
		return false, NewDatabasePrivilegeError(DatabasePrivilegePragma)
	}

	if !pragmaAllowed {
		return false, NewDatabasePrivilegeError(DatabasePrivilegePragma)
	}

	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegePragma) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegePragma)
}

// Determine if an Access Key is authorized to perform a read on a database.
func (accessKey *AccessKey) CanRead(databaseId, branchId, table, column string) (bool, error) {
	if accessKey.authorizedForColumn(databaseId, branchId, table, column, DatabasePrivilegeRead) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeRead)
}

// Determine if an Access Key is authorized to perform a recursive on a database.
func (accessKey *AccessKey) CanRecursive(databaseId, branchId string) (bool, error) {
	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegeRecursive) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeRecursive)
}

// Determine if an Access Key is authorized to perform a reindex on a database.
func (accessKey *AccessKey) CanReindex(databaseId, branchId, index string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, index, DatabasePrivilegeReindex) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeReindex)
}

// Determine if an Access Key is authorized to perform a savepoint on a database.
func (accessKey *AccessKey) CanSavepoint(databaseId, branchId, operation, savepoint string) (bool, error) {
	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegeSavepoint) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeSavepoint)
}

// Determine if an Access Key is authorized to perform a select on a database.
func (accessKey *AccessKey) CanSelect(databaseId, branchId string) (bool, error) {
	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegeSelect) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeSelect)
}

// Determine if an Access Key is authorized to perform a transaction on a database.
func (accessKey *AccessKey) CanTransaction(databaseId, branchId, operation string) (bool, error) {
	if accessKey.authorizedForBranch(databaseId, branchId, DatabasePrivilegeTransaction) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeTransaction)
}

// Determine if an Access Key is authorized to perform an update on a database.
func (accessKey *AccessKey) CanUpdate(databaseId, branchId, table, column string) (bool, error) {
	if accessKey.authorizedForColumn(databaseId, branchId, table, column, DatabasePrivilegeUpdate) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeUpdate)
}
