package test

import (
	"litebase/server"
	"litebase/server/storage"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

type TestServer struct {
	Address string
	App     *server.App
	Port    string
	Server  *httptest.Server
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

	t.Setenv("LITEBASE_PORT", port)
	app := server.NewApp(serveMux)
	app.Run()

	err := app.Cluster.Node().Start()

	if err != nil {
		log.Fatalf("Node start: %v", err)
	}

	t.Cleanup(func() {
		app.Cluster.Node().Shutdown()
		app.DatabaseManager.ConnectionManager().Shutdown()
		storage.Shutdown()
	})

	return &TestServer{
		Address: ts.URL,
		App:     app,
		Port:    port,
		Server:  ts,
	}
}
