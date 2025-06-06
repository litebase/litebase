package auth

type DatabasePrivilege string

const (
	DatabasePrivilegeAnalyze           DatabasePrivilege = "ANALYZE"
	DatabasePrivilegeAttach            DatabasePrivilege = "ATTACH"
	DatabasePrivilegeAlterTable        DatabasePrivilege = "ALTER_TABLE"
	DatabasePrivilegeCreateIndex       DatabasePrivilege = "CREATE_INDEX"
	DatabasePrivilegeCreateTable       DatabasePrivilege = "CREATE_TABLE"
	DatabasePrivilegeCreateTempTable   DatabasePrivilege = "CREATE_TEMP_TABLE"
	DatabasePrivilegeCreateTempTrigger DatabasePrivilege = "CREATE_TEMP_TRIGGER"
	DatabasePrivilegeCreateTempView    DatabasePrivilege = "CREATE_TEMP_VIEW"
	DatabasePrivilegeCreateTrigger     DatabasePrivilege = "CREATE_TRIGGER"
	DatabasePrivilegeCreateView        DatabasePrivilege = "CREATE_VIEW"
	DatabasePrivilegeCreateVTable      DatabasePrivilege = "CREATE_VTABLE"
	DatabasePrivilegeDelete            DatabasePrivilege = "DELETE"
	DatabasePrivilegeDetach            DatabasePrivilege = "DETACH"
	DatabasePrivilegeDropIndex         DatabasePrivilege = "DROP_INDEX"
	DatabasePrivilegeDropTable         DatabasePrivilege = "DROP_TABLE"
	DatabasePrivilegeDropTrigger       DatabasePrivilege = "DROP_TRIGGER"
	DatabasePrivilegeDropView          DatabasePrivilege = "DROP_VIEW"
	DatabasePrivilegeFunction          DatabasePrivilege = "FUNCTION"
	DatabasePrivilegeInsert            DatabasePrivilege = "INSERT"
	DatabasePrivilegePragma            DatabasePrivilege = "PRAGMA"
	DatabasePrivilegeRead              DatabasePrivilege = "READ"
	DatabasePrivilegeRecursive         DatabasePrivilege = "RECURSIVE"
	DatabasePrivilegeReindex           DatabasePrivilege = "REINDEX"
	DatabasePrivilegeSavepoint         DatabasePrivilege = "SAVEPOINT"
	DatabasePrivilegeSelect            DatabasePrivilege = "SELECT"
	DatabasePrivilegeTransaction       DatabasePrivilege = "TRANSACTION"
	DatabasePrivilegeUpdate            DatabasePrivilege = "UPDATE"
)
