package auth

import "strings"

type AccessKeyEffect string

const (
	AccessKeyEffectAllow AccessKeyEffect = "allow"
	AccessKeyEffectDeny  AccessKeyEffect = "deny"
)

// Determines if the AccessKeyEffect is valid.
func (e AccessKeyEffect) IsValid() bool {
	switch strings.ToLower(string(e)) {
	case string(AccessKeyEffectAllow), string(AccessKeyEffectDeny):
		return true
	default:
		return false
	}
}
