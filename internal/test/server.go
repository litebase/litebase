package test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/litebase/litebase/server/storage"

	"github.com/litebase/litebase/common/config"

	"github.com/litebase/litebase/server"
)

type TestServer struct {
	Address string
	App     *server.App
	Port    string
	Server  *httptest.Server
}

func NewTestQueryNode(t *testing.T) *TestServer {
	t.Setenv("LITEBASE_NODE_TYPE", config.NodeTypeQuery)

	return NewTestServer(t)
}

/*
NewTestServer creates a new test server, that fully initializes a node and
encapsulates the state of the node.
*/
func NewTestServer(t *testing.T) *TestServer {
	serveMux := http.NewServeMux()
	ts := httptest.NewServer(serveMux)
	port := ts.URL[len(ts.URL)-5:]

	t.Setenv("LITEBASE_PORT", port)

	configInstance := config.NewConfig()
	app := server.NewApp(configInstance, serveMux)
	app.Run()

	err := app.Cluster.Node().Start()

	if err != nil {
		log.Fatalf("Node start: %v", err)
	}

	server := &TestServer{
		Address: ts.URL[7:],
		App:     app,
		Port:    port,
		Server:  ts,
	}

	// t.Cleanup(func() {
	// 	server.Shutdown()
	// })

	return server
}

/*
Create a new test server that is not started. This is useful for testing
scenarios where the server needs to be started in a specific way.
*/
func NewUnstartedTestServer(t *testing.T) *TestServer {
	serveMux := http.NewServeMux()
	ts := httptest.NewServer(serveMux)
	port := ts.URL[len(ts.URL)-5:]

	t.Setenv("LITEBASE_PORT", port)

	configInstance := config.NewConfig()
	app := server.NewApp(configInstance, serveMux)
	app.Run()

	server := &TestServer{
		Address: ts.URL,
		App:     app,
		Port:    port,
		Server:  ts,
	}

	t.Cleanup(func() {
		// server.Shutdown()
	})

	return server
}

func (ts *TestServer) Shutdown() {
	ts.App.Cluster.Node().Shutdown()
	ts.App.DatabaseManager.ConnectionManager().Shutdown()
	storage.Shutdown(ts.App.Config)
}
