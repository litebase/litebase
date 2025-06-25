package http_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

// MockReadCloser is a simple mock implementation for testing
type MockReadCloser struct {
	io.Reader
}

func (m *MockReadCloser) Close() error {
	return nil
}

func TestRequestSignatureValidator_ValidSignature(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		// Setup request data - mimic the working example
		method := "GET"
		path := "/"
		headers := map[string]string{
			"Host": databaseUrl,
		}
		body := []byte{}
		queryParams := map[string]string{}

		// Generate valid signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create HTTP request
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: path,
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {token},
			},
		}

		// Create Request wrapper
		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		// Test signature validation
		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected valid signature to pass validation")
		}
	})
}

func TestRequestSignatureValidator_InvalidSignature(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		// Setup request data with correct signature
		method := "POST"
		path := "/api/test"
		headers := map[string]string{
			"Content-Type": "application/json",
			"Host":         databaseUrl,
		}
		body := []byte(`{"key":"value","count":123}`)
		queryParams := map[string]string{}

		// Generate valid signature for original body
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create HTTP request with modified body (invalid signature)
		jsonBody := `{"key":"modified","count":456}` // Different from signed body
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: path,
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization":  {token},
				"Content-Type":   {"application/json"},
				"Content-Length": {"30"},
			},
			Body: &MockReadCloser{
				Reader: bytes.NewReader([]byte(jsonBody)),
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		// Test signature validation
		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if isValid {
			t.Error("Expected invalid signature to fail validation")
		}
	})
}

func TestRequestSignatureValidator_NoContentLength(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		// Setup request data without body
		method := "GET"
		path := "/api/test"
		headers := map[string]string{
			"Host": databaseUrl,
		}
		body := []byte{}
		queryParams := map[string]string{
			"param": "value",
		}

		// Generate valid signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create HTTP request without Content-Length header
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path:     path,
				RawQuery: "param=value",
				Host:     databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {token},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		// Test signature validation
		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected request without Content-Length to pass validation")
		}
	})
}

func TestRequestSignatureValidator_InvalidRequestToken(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create HTTP request with invalid authorization header
		baseRequest := &http.Request{
			Method: "GET",
			URL: &url.URL{
				Path: "/api/test",
			},
			Header: map[string][]string{
				"Authorization": {"invalid-token"},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		// Test signature validation
		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if isValid {
			t.Error("Expected invalid request token to fail validation")
		}
	})
}

func TestRequestSignatureValidator_MissingAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a request token with non-existent access key
		nonExistentAccessKeyId := "non-existent-key-id"

		// Manually create a token structure (this will fail secret lookup)
		token := auth.SignRequest(
			nonExistentAccessKeyId,
			"fake-secret",
			"GET",
			"/api/test",
			map[string]string{"X-LBDB-Date": "20240101T000000Z"},
			[]byte{},
			map[string]string{},
		)

		baseRequest := &http.Request{
			Method: "GET",
			URL: &url.URL{
				Path: "/api/test",
			},
			Header: map[string][]string{
				"Authorization": {token},
				"X-LBDB-Date":   {"20240101T000000Z"},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		// Test signature validation
		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if isValid {
			t.Error("Expected missing access key to fail validation")
		}
	})
}

func TestRequestSignatureValidator_EmptyBody(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "POST"
		path := "/api/test"
		headers := map[string]string{
			"Content-Type": "application/json",
			"Host":         databaseUrl,
		}
		body := []byte{} // Empty body
		queryParams := map[string]string{}

		// Generate valid signature for empty body
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: path,
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization":  {token},
				"Content-Type":   {"application/json"},
				"Content-Length": {"0"},
			},
			Body: &MockReadCloser{
				Reader: bytes.NewReader([]byte("")),
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected empty body request to pass validation")
		}
	})
}

func TestRequestSignatureValidator_CaseInsensitiveHeaders(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "GET"
		path := "/api/test"
		headers := map[string]string{
			"Content-Type": "application/json",
			"Host":         databaseUrl,
		}
		body := []byte{}
		queryParams := map[string]string{}

		// Generate signature with standard headers
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create request with different case headers
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: path,
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization":  {token},
				"content-type":   {"application/json"}, // lowercase
				"Content-Length": {"0"},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected case-insensitive headers to pass validation")
		}
	})
}

func TestRequestSignatureValidator_PathNormalization(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "GET"
		path := "api/test" // No leading slash
		headers := map[string]string{
			"Host": databaseUrl,
		}
		body := []byte{}
		queryParams := map[string]string{}

		// Generate signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create request with different path format
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: "/api/test", // With leading slash
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {token},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected path normalization to work correctly")
		}
	})
}

func TestRequestSignatureValidator_ComplexQueryParams(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "GET"
		path := "/api/search"
		headers := map[string]string{
			"Host": databaseUrl,
		}
		body := []byte{}
		queryParams := map[string]string{
			"Query":  "test search",
			"Limit":  "10",
			"Offset": "0",
			"Sort":   "created_at",
		}

		// Generate signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path:     path,
				RawQuery: "Query=test+search&Limit=10&Offset=0&Sort=created_at",
				Host:     databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {token},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected complex query parameters to pass validation")
		}
	})
}

func TestRequestSignatureValidator_WithBodyContent(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "POST"
		path := "/api/data"
		headers := map[string]string{
			"Content-Type": "application/json",
			"Host":         databaseUrl,
		}
		body := []byte(`{"name":"test","value":42,"active":true,"tags":["tag1","tag2"],"nested":{"inner":"value"}}`)
		queryParams := map[string]string{}

		// Generate valid signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			body,
			queryParams,
		)

		// Create HTTP request with matching body
		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path: path,
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization":  {token},
				"Content-Type":   {"application/json"},
				"Content-Length": {fmt.Sprintf("%d", len(body))},
			},
			Body: &MockReadCloser{
				Reader: bytes.NewReader([]byte(body)),
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected request with complex body to pass validation")
		}
	})
}

func TestRequestSignatureValidator_EmptyToken(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		databaseUrl := "test.cluster.region.litebase.test"

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: "GET",
			URL: &url.URL{
				Path: "/api/test",
				Host: databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {""}, // Empty token
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if isValid {
			t.Error("Expected empty token to fail validation")
		}
	})
}

func TestRequestSignatureValidator_MissingAuthHeader(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		databaseUrl := "test.cluster.region.litebase.test"

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: "GET",
			URL: &url.URL{
				Path: "/api/test",
				Host: databaseUrl,
			},
			Header: map[string][]string{}, // No Authorization header
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if isValid {
			t.Error("Expected missing authorization header to fail validation")
		}
	})
}

func TestRequestSignatureValidator_SpecialCharacters(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		method := "GET"
		path := "/api/test"
		headers := map[string]string{
			"Host": databaseUrl,
		}

		queryParams := map[string]string{
			"query":   "hello world & more",
			"special": "chars!@#$%^&*()",
			"unicode": "测试",
			"encoded": "hello%20world",
		}

		// Generate valid signature
		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			nil,
			queryParams,
		)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: method,
			URL: &url.URL{
				Path:     path,
				RawQuery: "query=hello+world+%26+more&special=chars%21%40%23%24%25%5E%26%2A%28%29&unicode=%E6%B5%8B%E8%AF%95&encoded=hello%2520world",
				Host:     databaseUrl,
			},
			Header: map[string][]string{
				"Authorization": {token},
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		isValid := appHttp.RequestSignatureValidator(request, "Authorization")

		if !isValid {
			t.Error("Expected request with special characters to pass validation")
		}
	})
}
