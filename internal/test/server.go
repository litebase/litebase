package test

import (
	"litebase/server"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

type TestServer struct {
	Server *httptest.Server
}

/*
NewTestServer creates a new test server, that fully initializes a node and
encapsulates the state of the node.
*/
func NewTestServer(t *testing.T) *TestServer {
	serveMux := http.NewServeMux()
	ts := httptest.NewServer(serveMux)
	port := ts.URL[len(ts.URL)-5:]

	log.Println("Litebase Test Server running on port", port)
	{

		t.Setenv("LITEBASE_PORT", port)
		app := server.NewApp(serveMux)
		app.Run()
	}

	// s := server.NewServer()

	// s.Start(func(s *server.Server) {
	// })
	// ts.Start()
	return &TestServer{
		Server: ts,
	}
}
