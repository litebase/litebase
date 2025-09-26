package auth

import (
	"slices"
)

type Statement struct {
	Effect   StatementEffect `json:"effect" validate:"required,validateFn=IsValid" example:"allow" description:"Allow or deny effect for the statement"`
	Resource Resource        `json:"resource" validate:"required,validateFn=IsValid" example:"database:*" description:"Resource identifier or pattern"`
	Actions  []Privilege     `json:"actions" validate:"required,min=1,max=100" example:"read,write" description:"List of privileges/actions allowed or denied"`
}

// This method validates if all of the actions in the statement align with the
// selected resource.
func (s Statement) IsValid() bool {
	if s.Resource == "*" {
		return true
	}

	// Ensure that all of the actions can be applied to the resource. For example,
	// if the resource is "access-key:*"  or "access-key:<id>" then the actions
	// must all be scoped to the access key resource.
	for key, action := range Resources {
		if s.Resource.HasPrefix(key) {
			for _, aksAction := range s.Actions {
				if !slices.Contains(action, string(aksAction)) {
					return false
				}
			}

			return true
		}
	}

	return false
}
