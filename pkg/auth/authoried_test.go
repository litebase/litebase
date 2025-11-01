package auth_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/auth"
)

func TestAuthorized(t *testing.T) {
	testCases := []struct {
		name       string
		statements []auth.Statement
		resource   string
		action     auth.Privilege
		expected   bool
	}{
		{
			name: "Allow all actions on all resources",
			statements: []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
			resource: "*",
			action:   "read",
			expected: true,
		},
		{
			name: "Deny all actions on all resources",
			statements: []auth.Statement{
				{Effect: "Deny", Resource: "*", Actions: []auth.Privilege{"*"}},
			},
			resource: "*",
			action:   "write",
			expected: false,
		},
		{
			name: "Allow specific action on specific resource",
			statements: []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "resource1", Actions: []auth.Privilege{"read"}},
			},
			resource: "resource1",
			action:   "read",
			expected: true,
		},
		{
			name: "Deny specific action on specific resource",
			statements: []auth.Statement{
				{Effect: "Deny", Resource: "resource1", Actions: []auth.Privilege{"write"}},
			},
			resource: "resource1",
			action:   "write",
			expected: false,
		},
		{
			name: "Allow action on resource with wildcard",
			statements: []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "resource:*", Actions: []auth.Privilege{"read"}},
			},
			resource: "resource:123",
			action:   "read",
			expected: true,
		},
		{
			name: "Deny action on resource with wildcard",
			statements: []auth.Statement{
				{Effect: "Deny", Resource: "resource:*", Actions: []auth.Privilege{"write"}},
			},
			resource: "resource123",
			action:   "write",
			expected: false,
		},
		{
			name: "Allow action on specific resource with wildcard",
			statements: []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "resource1", Actions: []auth.Privilege{"read"}},
			},
			resource: "resource1",
			action:   "read",
			expected: true,
		},
		{
			name: "Deny action on specific resource with wildcard",
			statements: []auth.Statement{
				{Effect: "Deny", Resource: "resource1", Actions: []auth.Privilege{"write"}},
			},
			resource: "resource1",
			action:   "write",
			expected: false,
		},
		{
			name: "Allow action on specific resource with parent wildcard",
			statements: []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "a:*", Actions: []auth.Privilege{"write"}},
			},
			resource: "a:b:c:d",
			action:   "write",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := auth.Authorized(tc.statements, tc.resource, tc.action)

			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
