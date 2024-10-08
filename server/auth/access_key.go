package auth

import (
	"encoding/json"
	"fmt"
	_auth "litebase/internal/auth"
	"litebase/internal/config"
	"litebase/server/storage"
	"log"
	"strings"
)

type AccessKey struct {
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
	// Privileges      AccessKeyPrivilegeGroups `json:"privileges"`
	Permissions []*AccessKeyPermission `json:"permissions"`
}

type AccessKeyPrivilegeGroups map[string][]string
type AccessKeyPrivileges []string

//	Access Key Permissions are a list of resources and actions that can be performed on those resources.
//
// A "*" for the resource means all resources. A single action of "*" means all actions.
// Resources are databases, branches, and tables.
type AccessKeyPermission struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

func (accessKey AccessKey) authorized(resource string, action string) bool {
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

		if a == action {
			return true
		}
	}

	return false
}

func (accessKey AccessKey) authorizedForDatabase(databaseId, branchId, action string) bool {
	if accessKey.authorized(fmt.Sprintf("%s:%s", databaseId, "*"), action) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("%s:%s", databaseId, branchId), action)
}

func (accessKey AccessKey) authorizedForTable(databaseId, branchId, table, action string) bool {
	if accessKey.authorized(fmt.Sprintf("%s:%s:%s", databaseId, "*", table), action) {
		return true
	}

	if accessKey.authorized(fmt.Sprintf("%s:%s:%s", databaseId, branchId, "*"), action) {
		return true
	}

	return accessKey.authorized(fmt.Sprintf("%s:%s:%s", databaseId, branchId, table), action)
}

func (accessKey AccessKey) CanAccess(databaseId, branchId string) error {
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

func (accessKey AccessKey) CanAlter(databaseId, branchId string, args []string) (bool, error) {
	table := args[0]

	if table == "sqlite_master" {
		passes, _ := accessKey.CanIndex(databaseId, branchId, []string{table})

		if passes {
			return true, nil
		}
	}

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:ALTER") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("ALTER")
}

func (accessKey AccessKey) CanCreate(databaseId, branchId string, args []string) (bool, error) {
	table, _, databaseName := args[0], args[1], args[2]

	if accessKey.authorizedForDatabase(databaseId, branchId, "table:CREATE") {
		return true, nil
	}

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		passes, _ := accessKey.CanAlter(databaseId, branchId, []string{table})

		if passes {
			return true, nil
		}

		passes, _ = accessKey.CanDrop(databaseId, branchId, []string{table})

		if passes {
			return true, nil
		}

		if databaseName == "main" || databaseName == "temp" {
			return accessKey.CanTrigger(databaseId, branchId, []string{table, databaseName})
		}
	}

	return false, NewDatabasePrivilegeError("CREATE")
}

func (accessKey AccessKey) CanDelete(databaseId, branchId string, args []string) (bool, error) {
	table, _, databaseName := args[0], args[1], args[2]

	if databaseName == "main" || databaseName == "temp" {
		passes, _ := accessKey.CanDrop(databaseId, branchId, []string{table})

		if passes {
			return true, nil
		}
	}

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		passes, _ := accessKey.CanDrop(databaseId, branchId, []string{table})

		if passes {
			return true, nil
		}
	}

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:DELETE") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("DELETE")
}

func (accessKey AccessKey) CanDrop(databaseId, branchId string, args []string) (bool, error) {
	table := args[0]

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:DROP") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("DROP")
}

func (accessKey AccessKey) CanIndex(databaseId, branchId string, args []string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, "table:INDEX") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("INDEX")
}

func (accessKey AccessKey) CanInsert(databaseId, branchId string, args []string) (bool, error) {
	table := args[0]

	if table == "sqlite_master" {
		canCreate, _ := accessKey.CanCreate(databaseId, branchId, args)
		canIndex, _ := accessKey.CanIndex(databaseId, branchId, args)

		if canCreate || canIndex {
			return true, nil
		}
	}

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:INSERT") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("INSERT")
}

func (accessKey AccessKey) CanPragma(databaseId, branchId string, args []string) (bool, error) {
	// pragma, value := args[0].(string), args[1].(string)

	if accessKey.authorizedForDatabase(databaseId, branchId, "table:PRAGMA") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("PRAGMA")
}

func (accessKey AccessKey) CanRead(databaseId, branchId string, args []string) (bool, error) {
	return true, nil
}

func (accessKey AccessKey) CanSelect(databaseId, branchId string, args []string) (bool, error) {
	if accessKey.authorizedForDatabase(databaseId, branchId, "table:SELECT") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("SELECT")
}

func (accessKey AccessKey) CanTrigger(databaseId, branchId string, args []string) (bool, error) {
	_, table := args[0], args[1]

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:TRIGGER") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("TRIGGER")
}

func (accessKey AccessKey) CanUpdate(databaseId, branchId string, args []string) (bool, error) {
	table := args[0]

	if table == "sqlite_master" || table == "sqlite_temp_master" {
		return true, nil
	}

	if accessKey.authorizedForTable(databaseId, branchId, table, "table:UPDATE") {
		return true, nil
	}

	return false, NewDatabasePrivilegeError("UPDATE")
}

func (accessKey AccessKey) Delete() error {
	signatures := _auth.AllSignatures()

	for _, signature := range signatures {
		path := SecretsManager().SecretsPath(signature, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId))
		err := storage.ObjectFS().Remove(path)

		if err != nil {
			log.Println(err)
			// return err
		}
	}

	AccessKeyManager().Purge(accessKey.AccessKeyId)

	accessKey = AccessKey{}

	return nil
}

func (accessKey AccessKey) Update(
	privileges interface{},
) bool {
	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	storage.ObjectFS().WriteFile(
		SecretsManager().SecretsPath(config.Get().Signature, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId)),
		jsonValue,
		0666,
	)

	AccessKeyManager().Purge(accessKey.AccessKeyId)

	return true
}
