package auth_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/auth"
)

func TestResource(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		expected bool
	}{
		{"Valid Resource", "*", true},
		{"Valid Resource", "access-key", true},
		{"Valid Resource", "access-key:*", true},
		{"Valid Resource", "access-key:foobar", true},
		{"Invalid Resource", "invalid:resource", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := auth.Resource(tt.resource)

			if got := resource.IsValid(); got != tt.expected {
				t.Errorf("IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}
