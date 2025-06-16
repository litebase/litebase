package test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

type TestServer struct {
	Address string
	Client  *TestClient
	App     *server.App
	Port    string
	Server  *httptest.Server
	Started chan bool
}

/*
NewTestServer creates a new test server, that fully initializes a node and
encapsulates the state of the node.
*/
func NewTestServer(t testing.TB) *TestServer {
	serveMux := http.NewServeMux()
	ts := httptest.NewServer(serveMux)
	port := ts.URL[len(ts.URL)-5:]

	t.Setenv("LITEBASE_PORT", port)

	configInstance := config.NewConfig()
	app := server.NewApp(configInstance, serveMux)
	app.Run()

	server := &TestServer{
		Address: ts.URL[7:],
		App:     app,
		Port:    port,
		Server:  ts,
		Started: app.Cluster.Node().Start(),
	}

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

func (ts *TestServer) WithAccessKeyClient(statements []auth.AccessKeyStatement) *TestClient {
	if ts.Client == nil {
		accessKey := &auth.AccessKey{
			AccessKeyId:     CreateHash(32),
			AccessKeySecret: "accessKeySecret",
			Statements:      statements,
		}

		err := ts.App.Auth.SecretsManager.StoreAccessKey(accessKey)

		if err != nil {
			panic(err)
		}

		ts.Client = &TestClient{
			AccessKey: accessKey,
			URL:       ts.Server.URL,
		}
	}

	return ts.Client
}

func (ts *TestServer) WithBasicAuthClient() *TestClient {
	if ts.Client == nil {
		ts.Client = &TestClient{
			AccessKey: nil, // No access key for basic auth
			Username:  ts.App.Config.RootUsername,
			Password:  ts.App.Config.RootPassword,
			URL:       ts.Server.URL,
		}
	}

	return ts.Client
}

func (ts *TestServer) Shutdown() {
	ts.App.DatabaseManager.ConnectionManager().Shutdown()
	err := ts.App.Cluster.Node().Shutdown()

	if err != nil {
		panic(err)
	}

	// This may not be neccesary since this will be used in side of test.Run()
	// storage.Shutdown(ts.App.Config)

	ts.Server.CloseClientConnections()
	ts.Server.Close()
}
