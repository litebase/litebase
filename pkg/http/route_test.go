package http_test

import (
	"net/http"
	"testing"
	"time"

	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestNewRoute(t *testing.T) {
	router := appHttp.NewRouter()

	route := appHttp.NewRoute(router, func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{}
	})

	if route == nil {
		t.Fatal("Failed to create route")
	}
}

func TestRoute_Handle(t *testing.T) {
	router := appHttp.NewRouter()
	var handleCalled bool

	router.GlobalMiddleware = []appHttp.Middleware{}

	route := appHttp.NewRoute(router, func(request *appHttp.Request) appHttp.Response {
		handleCalled = true
		return appHttp.Response{}
	})

	req, _ := http.NewRequest("GET", "/", nil)

	resp := route.Handle(&appHttp.Request{
		BaseRequest: req,
	})

	if resp.StatusCode != 0 {
		t.Fatalf("expected status code 0, got %d", resp.StatusCode)
	}

	if !handleCalled {
		t.Fatal("expected handler to be called")
	}
}

func TestRoute_Middleware(t *testing.T) {
	router := appHttp.NewRouter()
	var middlewareCalled bool

	router.GlobalMiddleware = []appHttp.Middleware{}

	route := appHttp.NewRoute(router, func(request *appHttp.Request) appHttp.Response {
		return appHttp.Response{}
	}).Middleware([]appHttp.Middleware{
		func(request *appHttp.Request) (newRequest *appHttp.Request, response appHttp.Response) {
			middlewareCalled = true
			return request, appHttp.Response{}
		},
	})

	req, _ := http.NewRequest("GET", "/", nil)

	resp := route.Handle(&appHttp.Request{
		BaseRequest: req,
	})

	if resp.StatusCode != 0 {
		t.Fatalf("expected status code 0, got %d", resp.StatusCode)
	}

	if !middlewareCalled {
		t.Fatal("expected middleware to be called")
	}
}

func TestRoute_Timeout(t *testing.T) {
	router := appHttp.NewRouter()

	router.GlobalMiddleware = []appHttp.Middleware{}

	route := appHttp.NewRoute(router, func(request *appHttp.Request) appHttp.Response {
		time.Sleep(10 * time.Millisecond)
		return appHttp.Response{}
	}).Timeout(5 * time.Millisecond)

	req, _ := http.NewRequest("GET", "/", nil)

	resp := route.Handle(&appHttp.Request{
		BaseRequest: req,
	})

	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("expected status code %d, got %d", http.StatusRequestTimeout, resp.StatusCode)
	}
}
