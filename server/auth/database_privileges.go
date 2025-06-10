package auth

type DatabasePrivilege string

const (
	DatabasePrivilegeAnalyze           DatabasePrivilege = "database:analyze"
	DatabasePrivilegeAttach            DatabasePrivilege = "database:attach"
	DatabasePrivilegeAlterTable        DatabasePrivilege = "database:alter_table"
	DatabasePrivilegeCreateIndex       DatabasePrivilege = "database:create_index"
	DatabasePrivilegeCreateTable       DatabasePrivilege = "database:create_table"
	DatabasePrivilegeCreateTempTable   DatabasePrivilege = "database:create_temp_table"
	DatabasePrivilegeCreateTempTrigger DatabasePrivilege = "database:create_temp_trigger"
	DatabasePrivilegeCreateTempView    DatabasePrivilege = "database:create_temp_view"
	DatabasePrivilegeCreateTrigger     DatabasePrivilege = "database:create_trigger"
	DatabasePrivilegeCreateView        DatabasePrivilege = "database:create_view"
	DatabasePrivilegeCreateVTable      DatabasePrivilege = "database:create_vtable"
	DatabasePrivilegeDelete            DatabasePrivilege = "database:delete"
	DatabasePrivilegeDetach            DatabasePrivilege = "database:detach"
	DatabasePrivilegeDropIndex         DatabasePrivilege = "database:drop_index"
	DatabasePrivilegeDropTable         DatabasePrivilege = "database:drop_table"
	DatabasePrivilegeDropTrigger       DatabasePrivilege = "database:drop_trigger"
	DatabasePrivilegeDropView          DatabasePrivilege = "database:drop_view"
	DatabasePrivilegeFunction          DatabasePrivilege = "database:function"
	DatabasePrivilegeInsert            DatabasePrivilege = "database:insert"
	DatabasePrivilegePragma            DatabasePrivilege = "database:pragma"
	DatabasePrivilegeRead              DatabasePrivilege = "database:read"
	DatabasePrivilegeRecursive         DatabasePrivilege = "database:recursive"
	DatabasePrivilegeReindex           DatabasePrivilege = "database:reindex"
	DatabasePrivilegeSavepoint         DatabasePrivilege = "database:savepoint"
	DatabasePrivilegeSelect            DatabasePrivilege = "database:select"
	DatabasePrivilegeTransaction       DatabasePrivilege = "database:transaction"
	DatabasePrivilegeUpdate            DatabasePrivilege = "database:update"
)
