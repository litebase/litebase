package test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	httpRouter "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

type TestServer struct {
	Address        string
	Client         *TestClient
	App            *server.App
	Port           string
	Server         *httptest.Server
	PrivateAddress string
	PrivateServer  *httptest.Server
	PrivatePort    string
	Started        chan bool
}

/*
NewTestServer creates a new test server, that fully initializes a node and
encapsulates the state of the node.
*/
func NewTestServer(t testing.TB) *TestServer {
	// Create public server
	publicMux := http.NewServeMux()
	publicServer := httptest.NewServer(publicMux)
	publicPort := publicServer.URL[len(publicServer.URL)-5:]

	// Create private server
	privateMux := http.NewServeMux()
	privateServer := httptest.NewServer(privateMux)
	privatePort := privateServer.URL[len(privateServer.URL)-5:]

	t.Setenv("LITEBASE_PORT", publicPort)
	t.Setenv("LITEBASE_PRIVATE_PORT", privatePort)

	configInstance := config.NewConfig()
	app := server.NewApp(configInstance, publicMux)

	// Set up public routes
	app.Run()

	// Set up private routes
	privateRouter := httpRouter.NewRouter()
	privateRouter.PrivateServer(app.Cluster, app.DatabaseManager, app.LogManager, privateMux)

	// Set the private port provider so the cluster knows about the private server
	cluster.SetPrivatePortProvider(func() int {
		port := privateServer.URL[len(privateServer.URL)-5:]
		if portInt, err := strconv.Atoi(port); err == nil {
			return portInt
		}
		return 0
	})

	// Set the public port provider so the cluster knows about the public server
	cluster.SetPublicPortProvider(func() int {
		port := publicServer.URL[len(publicServer.URL)-5:]
		if portInt, err := strconv.Atoi(port); err == nil {
			return portInt
		}
		return 0
	})

	server := &TestServer{
		Address:        publicServer.URL[7:],
		App:            app,
		Port:           publicPort,
		Server:         publicServer,
		PrivateAddress: privateServer.URL[7:],
		PrivateServer:  privateServer,
		PrivatePort:    privatePort,
		Started:        app.Cluster.Node().Start(),
	}

	// Give the node time to complete startup including OnStarted callback
	// This ensures migrations have run before tests proceed
	// Use a small timeout to avoid blocking forever if something goes wrong
	select {
	case <-server.Started:
		// Startup completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for node to start")
	}

	return server
}

/*
Create a new test server that is not started. This is useful for testing
scenarios where the server needs to be started in a specific way.
*/
func NewUnstartedTestServer(t *testing.T) *TestServer {
	// Create public server
	publicMux := http.NewServeMux()
	publicServer := httptest.NewServer(publicMux)
	publicPort := publicServer.URL[len(publicServer.URL)-5:]

	// Create private server
	privateMux := http.NewServeMux()
	privateServer := httptest.NewServer(privateMux)
	privatePort := privateServer.URL[len(privateServer.URL)-5:]

	t.Setenv("LITEBASE_PORT", publicPort)
	t.Setenv("LITEBASE_PRIVATE_PORT", privatePort)

	configInstance := config.NewConfig()
	app := server.NewApp(configInstance, publicMux)

	// Set up public routes
	app.Run()

	// Set up private routes
	privateRouter := httpRouter.NewRouter()
	privateRouter.PrivateServer(app.Cluster, app.DatabaseManager, app.LogManager, privateMux)

	// Set the private port provider so the cluster knows about the private server
	cluster.SetPrivatePortProvider(func() int {
		port := privateServer.URL[len(privateServer.URL)-5:]
		if portInt, err := strconv.Atoi(port); err == nil {
			return portInt
		}
		return 0
	})

	// Set the public port provider so the cluster knows about the public server
	cluster.SetPublicPortProvider(func() int {
		port := publicServer.URL[len(publicServer.URL)-5:]
		if portInt, err := strconv.Atoi(port); err == nil {
			return portInt
		}
		return 0
	})

	server := &TestServer{
		Address:        publicServer.URL[7:],
		App:            app,
		Port:           publicPort,
		Server:         publicServer,
		PrivateAddress: privateServer.URL[7:],
		PrivateServer:  privateServer,
		PrivatePort:    privatePort,
		// Note: Started channel is not set - this server is not started
	}

	return server
}

func (ts *TestServer) WithAccessKey(accessKey *auth.AccessKey) *TestClient {
	if ts.Client == nil {

		ts.Client = &TestClient{
			AccessKey: accessKey,
			URL:       ts.Server.URL,
		}
	}

	return ts.Client
}

func (ts *TestServer) WithAccessKeyClient(statements []auth.Statement) *TestClient {
	accessKey, err := ts.App.Auth.AccessKeyManager.Create("", statements)

	if err != nil {
		panic(err)
	}

	return &TestClient{
		AccessKey: accessKey,
		URL:       ts.Server.URL,
	}
}

func (ts *TestServer) WithBasicAuthClient() *TestClient {
	return &TestClient{
		AccessKey: nil, // No access key for basic auth
		Username:  ts.App.Config.RootUsername,
		Password:  ts.App.Config.RootPassword,
		URL:       ts.Server.URL,
	}
}

func (ts *TestServer) WithTokenClient(token *auth.Token) *TestClient {
	return &TestClient{
		Token: token,
		URL:   ts.Server.URL,
	}
}

func (ts *TestServer) Shutdown() {
	ts.App.Shutdown()
	ts.App.DatabaseManager.ConnectionManager().Shutdown()
	err := ts.App.Cluster.Node().Shutdown()

	if err != nil {
		panic(err)
	}

	// This may not be necessary since this will be used in side of test.Run()
	// storage.Shutdown(ts.App.Config)

	ts.Server.CloseClientConnections()
	ts.Server.Close()

	if ts.PrivateServer != nil {
		ts.PrivateServer.CloseClientConnections()
		ts.PrivateServer.Close()
	}
}
