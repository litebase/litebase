package http_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/http"
)

func TestJsonResponse(t *testing.T) {
	response := http.JsonResponse(map[string]any{"key": "value"}, 200, map[string]string{"X-Custom-Header": "CustomValue"})

	if response.StatusCode != 200 {
		t.Errorf("Expected status code 200, got %d", response.StatusCode)
	}

	if response.Body["key"] != "value" {
		t.Errorf("Expected body key 'value', got %v", response.Body["key"])
	}

	if response.Headers["Content-Type"] != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %s", response.Headers["Content-Type"])
	}

	if response.Headers["X-Custom-Header"] != "CustomValue" {
		t.Errorf("Expected X-Custom-Header 'CustomValue', got %s", response.Headers["X-Custom-Header"])
	}
}

func TestSuccessResponse(t *testing.T) {
	response := http.SuccessResponse("Operation successful", map[string]any{"id": 1}, 200)

	if response.StatusCode != 200 {
		t.Errorf("Expected status code 200, got %d", response.StatusCode)
	}

	if response.Body["status"] != "success" {
		t.Errorf("Expected status 'success', got %s", response.Body["status"])
	}

	if response.Body["message"] != "Operation successful" {
		t.Errorf("Expected message 'Operation successful', got %s", response.Body["message"])
	}

	data := response.Body["data"].(map[string]any)
	if data["id"] != 1 {
		t.Errorf("Expected data id 1, got %v", data["id"])
	}
}
