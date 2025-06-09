package auth_test

import (
	"testing"

	"github.com/litebase/litebase/server/auth"
)

func TestAccessKeyEffect(t *testing.T) {
	tests := []struct {
		name     string
		effect   string
		expected bool
	}{
		{"Valid Allow Effect", "allow", true},
		{"Valid Deny Effect", "deny", true},
		{"Invalid Effect", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effect := auth.AccessKeyEffect(tt.effect)

			if got := effect.IsValid(); got != tt.expected {
				t.Errorf("IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}
