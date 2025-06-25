package auth

import "time"

type User struct {
	Username   string               `json:"username"`
	Password   string               `json:"password"`
	Statements []AccessKeyStatement `json:"statements"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

type UserResponse struct {
	Username   string               `json:"username"`
	Statements []AccessKeyStatement `json:"statements"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

// Check if the user has authorization for the given resources and actions
func (u *User) AuthorizeForResource(resources []string, actions []Privilege) bool {
	hasAuthorization := false

	for _, action := range actions {
		for _, resource := range resources {
			if Authorized(u.Statements, resource, action) {
				hasAuthorization = true
				break // No need to check further if one action is authorized
			}
		}
	}

	return hasAuthorization
}
