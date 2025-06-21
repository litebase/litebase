package http_test

import (
	"errors"
	"testing"

	"github.com/litebase/litebase/pkg/http"
)

func TestBadRequestResponse(t *testing.T) {
	response := http.BadRequestResponse(errors.New("bad request"))

	if response.StatusCode != 400 {
		t.Errorf("Expected status code 400, got %d", response.StatusCode)
	}

	if response.Body["status"] != "error" {
		t.Errorf("Expected status 'error', got %s", response.Body["status"])
	}

	if response.Body["message"] != "Error: bad request" {
		t.Errorf("Expected message 'Error: bad request', got %s", response.Body["message"])
	}
}

func TestForbiddenResponse(t *testing.T) {
	response := http.ForbiddenResponse(errors.New("forbidden"))

	if response.StatusCode != 403 {
		t.Errorf("Expected status code 403, got %d", response.StatusCode)
	}

	if response.Body["status"] != "error" {
		t.Errorf("Expected status 'error', got %s", response.Body["status"])
	}

	if response.Body["message"] != "Error: forbidden" {
		t.Errorf("Expected message 'Error: forbidden', got %s", response.Body["message"])
	}
}

func TestNotFoundResponse(t *testing.T) {
	response := http.NotFoundResponse(errors.New("not found"))

	if response.StatusCode != 404 {
		t.Errorf("Expected status code 404, got %d", response.StatusCode)
	}

	if response.Body["status"] != "error" {
		t.Errorf("Expected status 'error', got %s", response.Body["status"])
	}

	if response.Body["message"] != "Error: not found" {
		t.Errorf("Expected message 'Error: not found', got %s", response.Body["message"])
	}
}

func TestServerErrorResponse(t *testing.T) {
	response := http.ServerErrorResponse(errors.New("internal server error"))

	if response.StatusCode != 500 {
		t.Errorf("Expected status code 500, got %d", response.StatusCode)
	}

	if response.Body["status"] != "error" {
		t.Errorf("Expected status 'error', got %s", response.Body["status"])
	}

	if response.Body["message"] != "Error: internal server error" {
		t.Errorf("Expected message 'Error: internal server error', got %s", response.Body["message"])
	}
}
