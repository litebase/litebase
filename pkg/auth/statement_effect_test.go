package auth_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/auth"
)

func TestStatementEffect(t *testing.T) {
	tests := []struct {
		name     string
		effect   string
		expected bool
	}{
		{"Valid Allow Effect", string(auth.StatementEffectAllow), true},
		{"Valid Deny Effect", string(auth.StatementEffectDeny), true},
		{"Invalid Effect", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effect := auth.StatementEffect(tt.effect)

			if got := effect.IsValid(); got != tt.expected {
				t.Errorf("IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}
