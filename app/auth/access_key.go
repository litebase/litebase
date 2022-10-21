package auth

import (
	"golang.org/x/exp/slices"
)

type AccessKey struct {
	DatabaseUuid          string                   `json:"database_uuid"`
	BranchUuid            string                   `json:"branch_uuid"`
	AccessKeyId           string                   `json:"access_key_id"`
	AccessKeySecret       string                   `json:"access_key_secret"`
	ServerAccessKeySecret string                   `json:"server_access_key_secret"`
	Privileges            AccessKeyPrivilegeGroups `json:"privileges"`
}

type AccessKeyPrivilegeGroups map[string][]string
type AccessKeyPrivileges []string

func (accessKey *AccessKey) CanAlter(args ...interface{}) (bool, error) {
	table := args[0].(string)

	if table == "sqlite_master" {
		passes, _ := accessKey.CanIndex(table, "")

		if passes {
			return true, nil
		}
	}

	if _, ok := accessKey.Privileges[table]; ok && (slices.Contains(accessKey.Privileges[table], "ALL") || slices.Contains(accessKey.Privileges[table], "ALTER")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "ALTER")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("ALTER")
}

func (accessKey *AccessKey) CanCreate(args ...interface{}) (bool, error) {
	table, _, databaseName := args[0], args[1], args[2]

	if _, ok := accessKey.Privileges[table.(string)]; ok && (slices.Contains(accessKey.Privileges[table.(string)], "ALL") || slices.Contains(accessKey.Privileges[table.(string)], "CREATE")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "CREATE")) {
		return true, nil
	}

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		passes, _ := accessKey.CanAlter(table.(string))

		if passes {
			return true, nil
		}

		passes, _ = accessKey.CanDrop(table.(string))

		if passes {
			return true, nil
		}

		if databaseName == "main" || databaseName == "temp" {
			return accessKey.CanTrigger(table.(string), databaseName.(string))
		}
	}

	return false, NewDatabasePrivilegeError("CREATE")
}

func (accessKey *AccessKey) CanDelete(args ...interface{}) (bool, error) {
	table, _, databaseName := args[0].(string), args[1], args[2].(string)

	if databaseName == "main" || databaseName == "temp" {
		passes, _ := accessKey.CanDrop(table)

		if passes {
			return true, nil
		}
	}

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		passes, _ := accessKey.CanDrop(table)

		if passes {
			return true, nil
		}
	}

	if _, ok := accessKey.Privileges[table]; ok && (slices.Contains(accessKey.Privileges[table], "ALL") || slices.Contains(accessKey.Privileges[table], "DELETE")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "DELETE")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("DELETE")
}

func (accessKey *AccessKey) CanDrop(args ...interface{}) (bool, error) {
	table := args[0].(string)

	if _, ok := accessKey.Privileges[table]; ok && (slices.Contains(accessKey.Privileges[table], "ALL") || slices.Contains(accessKey.Privileges[table], "DROP")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "DROP")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("DROP")
}

func (accessKey *AccessKey) CanIndex(args ...interface{}) (bool, error) {
	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "INDEX")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("INDEX")
}

func (accessKey *AccessKey) CanInsert(args ...interface{}) (bool, error) {
	table := args[0]

	if table == "sqlite_master" {
		canCreate, _ := accessKey.CanCreate(args)
		canIndex, _ := accessKey.CanIndex(args)

		if canCreate || canIndex {
			return true, nil
		}
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "INSERT")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("INSERT")
}

func (accessKey *AccessKey) CanPragma(args ...interface{}) (bool, error) {
	// pragma, value := args[0].(string), args[1].(string)

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "PRAGMA")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("PRAGMA")
}

func (accessKey *AccessKey) CanRead(args ...interface{}) (bool, error) {
	return true, nil
}

func (accessKey *AccessKey) CanSelect(args ...interface{}) (bool, error) {
	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "SELECT")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("SELECT")
}

func (accessKey *AccessKey) CanTrigger(args ...interface{}) (bool, error) {
	_, table := args[0].(string), args[1].(string)

	if _, ok := accessKey.Privileges[table]; ok && (slices.Contains(accessKey.Privileges[table], "ALL") || slices.Contains(accessKey.Privileges[table], "INSERT")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "INSERT")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("TRIGGER")
}

func (accessKey *AccessKey) CanUpdate(args ...interface{}) (bool, error) {
	table := args[0]

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		return true, nil
	}

	if _, ok := accessKey.Privileges[table.(string)]; ok && (slices.Contains(accessKey.Privileges[table.(string)], "ALL") || slices.Contains(accessKey.Privileges[table.(string)], "UPDATE")) {
		return true, nil
	}

	if _, ok := accessKey.Privileges["*"]; ok && (slices.Contains(accessKey.Privileges["*"], "ALL") || slices.Contains(accessKey.Privileges["*"], "UPDATE")) {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("UPDATE")
}

func (accessKey *AccessKey) GetAccessKeyId() string {
	return accessKey.AccessKeyId
}

func (accessKey *AccessKey) GetBranchUuid() string {
	return accessKey.BranchUuid
}

func (accessKey *AccessKey) GetDatabaseUuid() string {
	return accessKey.DatabaseUuid
}
