package auth_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

func TestUser(t *testing.T) {
	user := &auth.User{
		Username: "testuser",
		Password: "testpassword",
		Statements: []auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Ensure user password is not exposed in JSON
	userJSON, err := json.Marshal(user)

	if err != nil {
		t.Fatalf("failed to marshal user: %v", err)
	}

	var userMap map[string]any

	if err := json.Unmarshal(userJSON, &userMap); err != nil {
		t.Fatalf("failed to unmarshal user JSON: %v", err)
	}

	if _, ok := userMap["password"]; !ok {
		t.Error("user password should be included in JSON output")
	}
}

func TestUserResponse(t *testing.T) {
	user := &auth.UserResponse{
		Username: "testuser",
		Statements: []auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// Ensure user password is not exposed in JSON
	userJSON, err := json.Marshal(user)

	if err != nil {
		t.Fatalf("failed to marshal user: %v", err)
	}

	var userMap map[string]any

	if err := json.Unmarshal(userJSON, &userMap); err != nil {
		t.Fatalf("failed to unmarshal user JSON: %v", err)
	}

	if _, ok := userMap["password"]; ok {
		t.Error("user password should not be included in JSON output")
	}
}
