package auth

var AccessKeyResources = map[string][]string{
	"access-key": {
		string(AccessKeyPrivilegeCreate),
		string(AccessKeyPrivilegeDelete),
		string(AccessKeyPrivilegeList),
		string(AccessKeyPrivilegeUpdate),
	},
}
