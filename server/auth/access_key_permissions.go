package auth

import (
	"fmt"
	"log"
	"strings"
)

//	Access Key Permissions are a list of resources and actions that can be performed on those resources.
//
// A "*" for the resource means all resources. A single action of "*" means all actions.
// Resources are databases, branches, and tables.
type AccessKeyPermission struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

// Determine if an Access Key is authorized to perform an action on a resource.
func (accessKey AccessKey) authorized(resource string, privilige DatabasePrivilege) bool {
	var accessKeyPermission *AccessKeyPermission

	for _, permission := range accessKey.Permissions {
		if permission.Resource == "*" {
			accessKeyPermission = permission
			break
		}

		if permission.Resource == resource {
			accessKeyPermission = permission
			break
		}
	}

	if accessKeyPermission == nil {
		return false
	}

	for _, a := range accessKeyPermission.Actions {
		if a == "*" {
			return true
		}

		if DatabasePrivilege(a) == privilige {
			return true
		}
	}

	return false
}

func (accessKey *AccessKey) authorizedForColumn(databaseId, branchId, table, column string, privilege DatabasePrivilege) bool {
	if accessKey.authorized(fmt.Sprintf("database:%s:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:*", databaseId, branchId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:table:*", databaseId, branchId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:table:%s:column:*", databaseId, branchId, table), privilege) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:table:%s:column:%s", databaseId, branchId, table, column), privilege)
}

// Determine if an Access Key is authorized to perform an action on a database.
func (accessKey *AccessKey) authorizedForDatabase(databaseId, branchId string, privilege DatabasePrivilege) bool {
	if accessKey.authorized(fmt.Sprintf("database:%s:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:*", databaseId), privilege) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("database:%s:branch:%s", databaseId, branchId), privilege)
}

// Determine if an Access Key is authorized to perform an action on a table.
func (accessKey *AccessKey) authorizedForTable(databaseId, branchId, table string, privilege DatabasePrivilege) bool {
	if accessKey.authorized(fmt.Sprintf("database:%s:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:*", databaseId, branchId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:table:*", databaseId, branchId), privilege) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:table:%s", databaseId, branchId, table), privilege)
}

// Determine if an Access Key is authorized to perform an action on a module.
func (accessKey *AccessKey) authorizedForVTable(databaseId, branchId, module, vtable string, privilege DatabasePrivilege) bool {

	if accessKey.authorized(fmt.Sprintf("database:%s:*", databaseId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:*", databaseId, branchId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:module:*", databaseId, branchId), privilege) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:module:%s:*", databaseId, branchId, module), privilege) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("database:%s:branch:%s:module:%s:vtable:%s", databaseId, branchId, module, vtable), privilege)
}

// Determine if an Access Key is authorized to perform an action on a branch.
func (accessKey *AccessKey) CanAccessDatabase(databaseId, branchId string) error {
	if databaseId == "" || branchId == "" {
		return NewDatabaseAccessError()
	}

	for _, permission := range accessKey.Permissions {
		if permission.Resource == "*" {
			return nil
		}

		if strings.HasPrefix(permission.Resource, fmt.Sprintf("%s:%s", databaseId, branchId)) {
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
	// if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeAttach) {
	// 	return true, nil
	// }

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
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeCreateTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTable)
}

// Determine if an Access Key is authorized to perform a create temp index on a database.
func (accessKey *AccessKey) CanCreateTempIndex(databaseId, branchId, tableName, indexName string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, tableName, DatabasePrivilegeCreateTempIndex) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeCreateTempIndex)
}

// Determine if an Access Key is authorized to perform a create temp table on a database.
func (accessKey *AccessKey) CanCreateTempTable(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeCreateTempTable) {
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
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeCreateTempView) {
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
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeCreateView) {
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
	// if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeDetach) {
	// 	return true, nil
	// }

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

// Determine if an Access Key is authorized to perform a drop temp index on a database.
func (accessKey *AccessKey) CanDropTempIndex(databaseId, branchId, table, index string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropTempIndex) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTempIndex)
}

// Determine if an Access Key is authorized to perform a drop temp table on a database.
func (accessKey *AccessKey) CanDropTempTable(databaseId, branchId, table string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropTempTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTempTable)
}

// Determine if an Access Key is authorized to perform a drop temp trigger on a database.
func (accessKey *AccessKey) CanDropTempTrigger(databaseId, branchId, table, trigger string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, table, DatabasePrivilegeDropTempTrigger) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTempTrigger)
}

// Determine if an Access Key is authorized to perform a drop temp view on a database.
func (accessKey *AccessKey) CanDropTempView(databaseId, branchId, view string) (bool, error) {
	if accessKey.authorizedForTable(databaseId, branchId, view, DatabasePrivilegeDropTempView) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropTempView)
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

// Determine if an Access Key is authorized to perform a drop vtable on a database.
func (accessKey *AccessKey) CanDropVTable(databaseId, branchId, module, vtable string) (bool, error) {
	if accessKey.authorizedForVTable(databaseId, branchId, module, vtable, DatabasePrivilegeDropVTable) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeDropVTable)
}

// Determine if an Access Key is authorized to perform a function on a database.
func (accessKey *AccessKey) CanFunction(databaseId, branchId, function string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeFunction) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeFunction)
}

func (accessKey *AccessKey) CanIndex(databaseId, branchId string, args []string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, "table:INDEX") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("INDEX")
}

// Determine if an Access Key is authorized to perform a function on a database.
func (accessKey *AccessKey) CanInsert(databaseId, branchId, table string) (bool, error) {
	// log.Println("CanInsert", databaseId, branchId, table)
	if table == "sqlite_master" {
		// canCreate, _ := accessKey.CanCreateTable(databaseId, branchId, table)
		// canIndex, _ := accessKey.CanCreateIndex(databaseId, branchId, table)

		// if canCreate || canIndex {
		return true, nil
		// }
	}

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

	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegePragma) {
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
func (accessKey *AccessKey) CanRecursive(databaseId, branchId, operation string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeRecursive) {
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
	log.Println("CanSavepoint", databaseId, branchId, operation, savepoint)
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeSavepoint) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeSavepoint)
}

// Determine if an Access Key is authorized to perform a select on a database.
func (accessKey *AccessKey) CanSelect(databaseId, branchId string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeSelect) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeSelect)
}

// Determine if an Access Key is authorized to perform a transaction on a database.
func (accessKey *AccessKey) CanTransaction(databaseId, branchId, operation string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, DatabasePrivilegeTransaction) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeTransaction)
}

// Determine if an Access Key is authorized to perform an update on a database.
func (accessKey *AccessKey) CanUpdate(databaseId, branchId, table, column string) (bool, error) {

	// if table == "sqlite_master" || table == "sqlite_temp_master" {
	// 	return true, nil
	// }

	if accessKey.authorizedForColumn(databaseId, branchId, table, column, DatabasePrivilegeUpdate) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError(DatabasePrivilegeUpdate)
}
