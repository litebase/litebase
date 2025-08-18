package auth

import "strings"

type StatementEffect string

const (
	StatementEffectAllow StatementEffect = "allow"
	StatementEffectDeny  StatementEffect = "deny"
)

// Determines if the StatementEffect is valid.
func (e StatementEffect) IsValid() bool {
	switch strings.ToLower(string(e)) {
	case string(StatementEffectAllow), string(StatementEffectDeny):
		return true
	default:
		return false
	}
}
