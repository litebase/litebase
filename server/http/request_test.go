package http_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
	appHttp "github.com/litebase/litebase/server/http"
)

func TestNewRequest(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.BaseRequest != baseRequest {
			t.Errorf("expected BaseRequest to be %v, got %v", baseRequest, request.BaseRequest)
		}

		if request.Method != http.MethodGet {
			t.Errorf("expected Method to be %s, got %s", http.MethodGet, request.Method)
		}

		if request.Headers().Get("host") != "foo.bar.litebase.test" {
			t.Errorf("expected headers[host] to be %s, got %s", "foo.bar.litebase.test", request.Headers().Get("host"))
		}
	})
}

func TestRequest_All(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		buffer := bytes.NewBufferString(`{}`)
		body := io.NopCloser(buffer)

		baseRequest := &http.Request{
			Body:   body,
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if len(request.All()) != 0 {
			t.Errorf("expected All() to be empty map, got %v", request.All())
		}

		buffer = bytes.NewBufferString(`{"foo": "bar"}`)
		body = io.NopCloser(buffer)

		baseRequest = &http.Request{
			Body:   body,
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request = appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if len(request.All()) != 1 {
			t.Errorf("expected All() to contain one item, got %v", request.All())
		}

		if request.All()["foo"] != "bar" {
			t.Errorf("expected All()[foo] to be %s, got %s", "bar", request.All()["foo"])
		}
	})
}

func TestRequest_ClusterId(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: http.MethodGet,
			URL: &url.URL{
				Host: databaseUrl,
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.ClusterId() != app.Config.ClusterId {
			t.Errorf("expected ClusterId to be %s, got %s", app.Config.ClusterId, request.ClusterId())
		}
	})
}

func TestRequest_DatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: http.MethodGet,
			URL: &url.URL{
				Host: databaseUrl,
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.DatabaseKey() == nil {
			t.Errorf("expected DatabaseKey to be not nil, got nil")
		}

		if request.DatabaseKey().Key != db.DatabaseKey.Key {
			t.Errorf("expected DatabaseKey.Key to be %s, got %s", db.DatabaseKey.Key, request.DatabaseKey().Key)
		}
	})
}

func TestRequest_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		buffer := bytes.NewBufferString(`{}`)
		body := io.NopCloser(buffer)

		baseRequest := &http.Request{
			Body:   body,
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if len(request.All()) != 0 {
			t.Errorf("expected All() to be empty map, got %v", request.All())
		}

		buffer = bytes.NewBufferString(`{"foo": "bar"}`)
		body = io.NopCloser(buffer)

		baseRequest = &http.Request{
			Body:   body,
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request = appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Get("foo") != "bar" {
			t.Errorf("expected Get(foo) to be %s, got %s", "bar", request.Get("foo"))
		}
	})
}

func TestRequest_Headers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Headers().Get("host") != "foo.bar.litebase.test" {
			t.Errorf("expected headers[host] to be %s, got %s", "foo.bar.litebase.test", request.Headers().Get("host"))
		}
	})
}

type InputTest struct {
	Name  string
	Value string
}

func TestRequest_Input(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {

		buffer := bytes.NewBufferString(`{"name": "foo", "value": "bar"}`)
		body := io.NopCloser(buffer)

		baseRequest := &http.Request{
			Body:   body,
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		input, err := request.Input(&InputTest{})

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if input.(*InputTest).Name != "foo" {
			t.Errorf("expected Input.Name to be %s, got %s", "foo", input.(*InputTest).Name)
		}

		if input.(*InputTest).Value != "bar" {
			t.Errorf("expected Input.Value to be %s, got %s", "bar", input.(*InputTest).Value)
		}
	})
}

func TestRequest_Param(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Param("foo") != "" {
			t.Errorf("expected Param(foo) to be empty, got %s", request.Param("foo"))
		}

		baseRequest = &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
				Path: "/foo/bar",
			},
		}

		baseRequest.SetPathValue("foo", "bar")

		request = appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Param("foo") != "bar" {
			t.Errorf("expected Param(foo) to be %s, got %s", "bar", request.Param("foo"))
		}
	})
}

func TestRequest_Path(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Path() != "" {
			t.Errorf("expected Path() to be empty, got %s", request.Path())
		}

		baseRequest = &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.litebase.test",
				Path: "/foo/bar",
			},
		}

		request = appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Path() != "/foo/bar" {
			t.Errorf("expected Path() to be %s, got %s", "/foo/bar", request.Path())
		}
	})
}

func TestRequest_QueryParams(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "foo.bar.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host:     "foo.bar.litebase.test",
				RawQuery: "foo=bar",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.QueryParam("foo") != "bar" {
			t.Errorf("expected QueryParam(foo) to be %s, got %s", "bar", request.QueryParam("foo"))
		}
	})
}

func TestRequest_Region(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: http.MethodGet,
			URL: &url.URL{
				Host: databaseUrl,
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if request.Region() != app.Config.Region {
			t.Errorf("expected Region to be %s, got %s", app.Config.Region, request.Region())
		}
	})
}

func TestRequest_RequestToken(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		databaseUrl := fmt.Sprintf("%s.%s.%s.litebase.test", db.DatabaseKey.Key, app.Config.ClusterId, app.Config.Region)

		token := auth.SignRequest(
			db.AccessKey.AccessKeyId,
			db.AccessKey.AccessKeySecret,
			"GET",
			"/",
			map[string]string{},
			map[string]any{},
			map[string]string{},
		)

		baseRequest := &http.Request{
			Host:   databaseUrl,
			Method: http.MethodGet,
			Header: map[string][]string{
				"Authorization": {token},
			},
			URL: &url.URL{
				Host: databaseUrl,
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if !request.RequestToken("Authorization").Valid() {
			t.Errorf("expected RequestToken to be valid, got invalid")
		}
	})
}

func TestRequest_Subdomains(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		baseRequest := &http.Request{
			Host:   "litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if len(request.Subdomains()) != 0 {
			t.Errorf("expected Subdomains() to be empty, got %v", request.Subdomains())
		}

		baseRequest = &http.Request{
			Host:   "foo.bar.baz.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.baz.litebase.test",
			},
		}

		request = appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		if len(request.Subdomains()) != 3 {
			t.Errorf("expected Subdomains() to be 3, got %d", len(request.Subdomains()))
		}
	})
}

type TestValidationInput struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

func TestRequest_Validate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		buffer := bytes.NewBufferString(`{"key": ""}`)
		body := io.NopCloser(buffer)

		baseRequest := &http.Request{
			Body:   body,
			Host:   "foo.bar.us-east-1.litebase.test",
			Method: http.MethodGet,
			URL: &url.URL{
				Host: "foo.bar.us-east-1.litebase.test",
			},
		}

		request := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			baseRequest,
		)

		input, err := request.Input(&TestValidationInput{})

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		validationErrors := request.Validate(input, map[string]string{
			"key.required":   "The key field is required",
			"value.required": "The value field is required",
		})

		if len(validationErrors) <= 0 {
			t.Errorf("expected no validation errors, got %v", validationErrors)
		}
		if validationErrors["key"][0] != "The key field is required" {
			t.Errorf("expected validation error for key to be %s, got %s", "The key field is required", validationErrors["key"])
		}

		if validationErrors["value"][0] != "The value field is required" {
			t.Errorf("expected validation error for value to be %s, got %s", "The value field is required", validationErrors["value"])
		}
	})
}
