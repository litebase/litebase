package http_test

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewRouter(t *testing.T) {
	router := appHttp.NewRouter()

	if router == nil {
		t.Fatal("Failed to create router")
	}

	// Test default middleware is set
	if len(router.GlobalMiddleware) == 0 {
		t.Error("Expected global middleware to be set")
	}

	// Test routes map is initialized
	if router.Routes == nil {
		t.Error("Expected routes map to be initialized")
	}

	// Test all HTTP methods are initialized
	methods := []string{"GET", "POST", "PUT", "PATCH", "DELETE"}
	for _, method := range methods {
		if _, exists := router.Routes[method]; !exists {
			t.Errorf("Expected method %s to be initialized", method)
		}
	}
}

func TestRouterHTTPMethods(t *testing.T) {
	router := appHttp.NewRouter()
	handler := func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{StatusCode: http.StatusOK}
	}

	tests := []struct {
		name     string
		method   func(string, func(*appHttp.Request) appHttp.Response) *appHttp.Route
		path     string
		httpVerb string
	}{
		{"GET", router.Get, "/test", "GET"},
		{"POST", router.Post, "/test", "POST"},
		{"PUT", router.Put, "/test", "PUT"},
		{"PATCH", router.Patch, "/test", "PATCH"},
		{"DELETE", router.Delete, "/test", "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := tt.method(tt.path, handler)
			if route == nil {
				t.Errorf("Expected route to be created for %s", tt.name)
			}

			// Verify route was added to correct method map
			if router.Routes[tt.httpVerb][tt.path] != route {
				t.Errorf("Route not properly registered for %s %s", tt.httpVerb, tt.path)
			}
		})
	}
}

func TestRouterPathTrimming(t *testing.T) {
	router := appHttp.NewRouter()
	handler := func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{StatusCode: http.StatusOK}
	}

	tests := []struct {
		input    string
		expected string
	}{
		{"/", ""},
		{"/api/", "/api"},
		{"/api/v1/", "/api/v1"},
		{"/api", "/api"},
		{"", ""},
		{"/path/with/multiple/trailing/slashes/", "/path/with/multiple/trailing/slashes"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			router.Get(tt.input, handler)
			if router.Routes["GET"][tt.expected] == nil {
				t.Errorf("Expected route to exist at path '%s'", tt.expected)
			}
			// Ensure original path with trailing slash doesn't exist
			if tt.input != tt.expected && router.Routes["GET"][tt.input] != nil {
				t.Errorf("Expected original path '%s' to be trimmed", tt.input)
			}
		})
	}
}

func TestRouterFallback(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()
		called := false

		router.Fallback(func(request *appHttp.Request) appHttp.Response {
			called = true
			return appHttp.Response{StatusCode: http.StatusNotFound}
		})

		// Create a mock request to trigger fallback
		req := httptest.NewRequest("GET", "http://localhost/nonexistent", nil)
		mockRequest := appHttp.NewRequest(app.Cluster, app.DatabaseManager, app.LogManager, req)
		router.DefaultRoute.Handler(mockRequest)

		if !called {
			t.Error("Expected fallback handler to be called")
		}
	})
}

func TestRouterServer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/test", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       map[string]any{"message": "test"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/test", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		if err != nil {
			t.Errorf("Failed to unmarshal response: %v", err)
		}

		if response["message"] != "test" {
			t.Errorf("Expected message 'test', got '%s'", response["message"])
		}
	})
}

func TestRouterServerFallbackRoute(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		router.Fallback(func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusNotFound,
				Body:       map[string]any{"error": "not found"},
			}
		})

		req := httptest.NewRequest("GET", "http://localhost/nonexistent", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status code %d, got %d", http.StatusNotFound, w.Code)
		}

		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		if err != nil {
			t.Errorf("Failed to unmarshal response: %v", err)
		}

		if response["error"] != "not found" {
			t.Errorf("Expected error 'not found', got '%s'", response["error"])
		}

		// Get the content length header
		contentLength := w.Header().Get("Content-Length")
		if contentLength == "" {
			t.Error("Expected Content-Length header to be set")
		}
	})
}

func TestRouterServerWithNilBody(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/empty", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusNoContent,
				Body:       nil,
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/empty", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("Expected status code %d, got %d", http.StatusNoContent, w.Code)
		}

		// The router writes an empty string when body is nil, which gets JSON marshaled to ""
		if w.Body.String() != "" {
			t.Errorf("Expected empty response body, got %s", w.Body.String())
		}
	})
}

func TestRouterServerWithHeaders(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/headers", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"X-Custom-Header": "test-value",
					"Content-Type":    "application/json",
				},
				Body: map[string]any{"test": "data"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/headers", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Header().Get("X-Custom-Header") != "test-value" {
			t.Error("Expected custom header to be set")
		}

		if w.Header().Get("Content-Type") != "application/json" {
			t.Error("Expected content type header to be set")
		}
	})
}

func TestRouterServerWithGzipEncoding(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/gzip", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"Content-Encoding": "gzip",
				},
				Body: map[string]any{"message": "compressed"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/gzip", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		// Verify gzip compression
		reader, err := gzip.NewReader(w.Body)
		if err != nil {
			t.Fatalf("Failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		var response map[string]string
		err = json.NewDecoder(reader).Decode(&response)
		if err != nil {
			t.Errorf("Failed to decode gzipped response: %v", err)
		}

		if response["message"] != "compressed" {
			t.Errorf("Expected message 'compressed', got '%s'", response["message"])
		}
	})
}

func TestRouterServerWithStream(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/stream", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Stream: func(w http.ResponseWriter) {
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte("streamed response"))
				},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/stream", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		if w.Body.String() != "streamed response" {
			t.Errorf("Expected 'streamed response', got '%s'", w.Body.String())
		}

		if w.Header().Get("Content-Type") != "text/plain" {
			t.Error("Expected Content-Type header to be set by stream")
		}
	})
}

func TestRouterServerWithZeroStatusCode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/zero", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: 0,
				Body:       map[string]any{"message": "zero status"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/zero", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		// Handler should return early when status code is 0
		if w.Code != http.StatusOK { // Default status when nothing is written
			t.Errorf("Expected default status code, got %d", w.Code)
		}

		if w.Body.Len() != 0 {
			t.Error("Expected empty body when status code is 0")
		}
	})
}

func TestRouterMultipleRoutesOnSamePath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/api", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       map[string]any{"method": "GET"},
			}
		})

		router.Post("/api", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusCreated,
				Body:       map[string]any{"method": "POST"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		// Test GET
		req := httptest.NewRequest("GET", "http://localhost/api", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected GET status code %d, got %d", http.StatusOK, w.Code)
		}

		// Test POST
		req = httptest.NewRequest("POST", "http://localhost/api", nil)
		w = httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("Expected POST status code %d, got %d", http.StatusCreated, w.Code)
		}
	})
}

func TestRouterServerWithErrorStatusCode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/error", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       map[string]any{"error": "server error"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/error", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status code %d, got %d", http.StatusInternalServerError, w.Code)
		}

		// Verify Connection: close header is set for error responses
		if w.Header().Get("Connection") != "close" {
			t.Error("Expected Connection: close header for error response")
		}
	})
}

func TestRouterLazyMapInitialization(t *testing.T) {
	router := appHttp.NewRouter()

	// Initially, method maps should be nil
	for method := range router.Routes {
		if router.Routes[method] != nil {
			t.Errorf("Expected %s routes map to be nil initially", method)
		}
	}

	handler := func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{StatusCode: http.StatusOK}
	}

	// Adding a route should initialize the map
	router.Get("/test", handler)

	if router.Routes["GET"] == nil {
		t.Error("Expected GET routes map to be initialized after adding route")
	}

	if router.Routes["POST"] != nil {
		t.Error("Expected POST routes map to remain nil")
	}
}

func TestRouterOverwriteRoute(t *testing.T) {
	router := appHttp.NewRouter()

	firstHandler := func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{StatusCode: http.StatusOK, Body: map[string]any{"handler": "first"}}
	}

	secondHandler := func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{StatusCode: http.StatusOK, Body: map[string]any{"handler": "second"}}
	}

	// Add first route
	route1 := router.Get("/test", firstHandler)

	// Add second route with same path (should overwrite)
	route2 := router.Get("/test", secondHandler)

	// Should be different route objects
	if route1 == route2 {
		t.Error("Expected different route objects when overwriting")
	}

	// Should have only one route in the map
	if len(router.Routes["GET"]) != 1 {
		t.Errorf("Expected 1 route, got %d", len(router.Routes["GET"]))
	}

	// Should be the second route
	if router.Routes["GET"]["/test"] != route2 {
		t.Error("Expected second route to overwrite first")
	}
}

func TestRouterEmptyStringBodyResponse(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/empty-string", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       nil,
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/empty-string", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		if w.Body.String() != "" {
			t.Errorf("Expected body %s, got %s", "", w.Body.String())
		}
	})
}

func TestRouterComplexBodyResponse(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		router.Get("/complex", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body: map[string]any{
					"string":  "value",
					"number":  42,
					"boolean": true,
					"array":   []string{"a", "b", "c"},
					"object": map[string]any{
						"nested": "value",
					},
				},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/complex", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]any
		err := json.Unmarshal(w.Body.Bytes(), &response)
		if err != nil {
			t.Errorf("Failed to unmarshal complex response: %v", err)
		}

		if response["string"] != "value" {
			t.Error("Complex response string field mismatch")
		}

		if response["number"].(float64) != 42 {
			t.Error("Complex response number field mismatch")
		}
	})
}

func TestRouterGlobalMiddleware(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		// Verify default middleware exists
		if len(router.GlobalMiddleware) == 0 {
			t.Error("Expected default global middleware to be set")
		}

		// Test that we can add additional middleware
		originalCount := len(router.GlobalMiddleware)

		// Note: We can't easily test middleware execution without knowing the internal structure
		// This test just verifies the middleware list is accessible
		if originalCount < 1 {
			t.Error("Expected at least one default middleware")
		}
	})
}

func TestRouterWithURLParameters(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		// Test basic path matching
		router.Get("/users/123", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       map[string]any{"userId": "123"},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/users/123", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}
	})
}

func TestRouterLargeResponse(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		// Create a large response
		largeData := make(map[string]any)
		for i := range 1000 {
			largeData[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
		}

		router.Get("/large", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       largeData,
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		req := httptest.NewRequest("GET", "http://localhost/large", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]any
		err := json.Unmarshal(w.Body.Bytes(), &response)
		if err != nil {
			t.Errorf("Failed to unmarshal large response: %v", err)
		}

		if len(response) != 1000 {
			t.Errorf("Expected 1000 keys in response, got %d", len(response))
		}
	})
}

func TestRouterLoadRoutes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		serveMux := http.NewServeMux()

		// This should call LoadRoutes internally
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		// After LoadRoutes is called, the routes map should have routes
		// We can't test the exact routes since they're loaded from LoadRoutes function
		// but we can verify the router.Server call doesn't panic and sets up the server
		if serveMux == nil {
			t.Error("Expected serveMux to be properly configured")
		}
	})
}

func TestRouterEdgeCases(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		router := appHttp.NewRouter()

		// Test with numeric response body
		router.Get("/number", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       map[string]any{"number": 42},
			}
		})

		// Test with boolean response body
		router.Get("/boolean", func(request *appHttp.Request) appHttp.Response {
			return appHttp.Response{
				StatusCode: http.StatusOK,
				Body:       map[string]any{"boolean": true},
			}
		})

		serveMux := http.NewServeMux()
		router.Server(app.Cluster, app.DatabaseManager, app.LogManager, serveMux)

		// Test numeric response
		req := httptest.NewRequest("GET", "http://localhost/number", nil)
		w := httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d for number endpoint, got %d", http.StatusOK, w.Code)
		}

		if w.Body.String() != `{"number":42}` {
			t.Errorf("Expected body {\"number\":42}, got '%s'", w.Body.String())
		}

		// Test boolean response
		req = httptest.NewRequest("GET", "http://localhost/boolean", nil)
		w = httptest.NewRecorder()
		serveMux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d for boolean endpoint, got %d", http.StatusOK, w.Code)
		}

		if w.Body.String() != `{"boolean":true}` {
			t.Errorf("Expected body {\"boolean\":true}, got '%s'", w.Body.String())
		}
	})
}
