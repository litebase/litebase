package http_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/http"
)

func TestNewHeaders(t *testing.T) {
	headers := map[string]string{
		"Content-Type":    "application/json",
		"X-Custom-Header": "custom-value",
	}

	h := http.NewHeaders(headers)

	if len(h.All()) != 2 {
		t.Errorf("Expected 2 headers, got %d", len(h.All()))
	}

	if h.Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type to be application/json, got %s", h.Get("Content-Type"))
	}

	if h.Get("X-Custom-Header") != "custom-value" {
		t.Errorf("Expected X-Custom-Header to be custom-value, got %s", h.Get("X-Custom-Header"))
	}

	if !h.Has("Content-Type") {
		t.Error("Expected Has to return true for Content-Type")
	}

	if h.Has("Non-Existent-Header") {
		t.Error("Expected Has to return false for Non-Existent-Header")
	}
}
