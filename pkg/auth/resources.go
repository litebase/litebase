package auth

var Resources = map[string][]string{
	"access-key": {
		string(AccessKeyPrivilegeCreate),
		string(AccessKeyPrivilegeDelete),
		string(AccessKeyPrivilegeList),
		string(AccessKeyPrivilegeUpdate),
	},
}
