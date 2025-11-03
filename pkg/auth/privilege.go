package auth

type Privilege string

const (
	// PrivilegeWildcard represents all privileges (wildcard permission)
	PrivilegeWildcard Privilege = "*"
)
