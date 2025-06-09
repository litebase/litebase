package auth

import (
	"slices"
)

type AccessKeyStatement struct {
	Effect   AccessKeyEffect   `json:"effect" validate:"required,validateFn=IsValid"`
	Resource AccessKeyResource `json:"resource" validate:"required,validateFn=IsValid"`
	Actions  []string          `json:"actions" validate:"required,min=1,max=100"`
}

// This method validates if all of the actions in the statement align with the
// selected resource.
func (aks AccessKeyStatement) IsValid() bool {
	if aks.Resource == "*" {
		return true
	}

	// Ensure that all of the actions can be applied to the resource. For example,
	// if the resource is "access-key:*"  or "access-key:<id>" then the actions
	// must all be scoped to the access key resource.
	for key, action := range AccessKeyResources {
		if aks.Resource.HasPrefix(key) {
			for _, aksAction := range aks.Actions {
				if !slices.Contains(action, aksAction) {
					return false
				}
			}

			return true
		}
	}

	return false
}
