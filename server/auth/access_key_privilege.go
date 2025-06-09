package auth

type AccessKeyPrivilege string

const (
	AccessKeyPrivilegeCreate AccessKeyPrivilege = "access-key:create"
	AccessKeyPrivilegeDelete AccessKeyPrivilege = "access-key:delete"
	AccessKeyPrivilegeList   AccessKeyPrivilege = "access-key:list"
	AccessKeyPrivilegeUpdate AccessKeyPrivilege = "access-key:update"
)
