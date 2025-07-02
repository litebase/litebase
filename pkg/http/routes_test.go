package http_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	appHttp "github.com/litebase/litebase/pkg/http"
)

// RouteTestCase represents a test case for a specific route
type RouteTestCase struct {
	Method             string
	Path               string
	ExpectedMiddleware []string
	Description        string
}

// middlewareToString converts a middleware function to its name for comparison
func middlewareToString(middleware appHttp.Middleware) string {
	if middleware == nil {
		return "nil"
	}

	// Use reflection to get the function name
	funcName := runtime.FuncForPC(reflect.ValueOf(middleware).Pointer()).Name()

	// Extract just the function name from the full path
	parts := strings.Split(funcName, ".")

	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return "Unknown"
}

// convertMiddlewareSliceToNames converts a slice of middleware functions to their names
func convertMiddlewareSliceToNames(middleware []appHttp.Middleware) []string {
	names := make([]string, len(middleware))

	for i, mw := range middleware {
		names[i] = middlewareToString(mw)
	}

	return names
}

// TestRoutesMiddleware tests that all routes have the expected middleware applied
func TestRoutesMiddleware(t *testing.T) {
	// Define expected middleware for each route
	routeTests := []RouteTestCase{
		// Administrative routes
		{
			Method:             "GET",
			Path:               "/status",
			ExpectedMiddleware: []string{"RequireHost", "Authentication"},
			Description:        "Cluster status route should have RequireHost and Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/users",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "User index route should have Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/users/{username}",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "User show route should have Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/resources/users",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "User store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/resources/users/{username}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "User destroy route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/access-keys",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Access key index route should have Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/access-keys/{accessKeyId}",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Access key show route should have Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/resources/access-keys",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Access key store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "PUT",
			Path:               "/resources/access-keys/{accessKeyId}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Access key update route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/resources/access-keys/{accessKeyId}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Access key destroy route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/databases",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Database index route should have Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/resources/databases/{databaseId}",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Database show route should have Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/resources/databases",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Database store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/resources/databases/{databaseId}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Database destroy route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/resources/keys",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Key store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/resources/keys/activate",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Key activate route should have ForwardToPrimary and Authentication middleware",
		},
		// Internal cluster routes
		{
			Method:             "POST",
			Path:               "/cluster/connection",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Cluster connection route should have Internal middleware",
		},
		{
			Method:             "POST",
			Path:               "/cluster/election",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Cluster election route should have Internal middleware",
		},
		{
			Method:             "POST",
			Path:               "/cluster/members",
			ExpectedMiddleware: []string{},
			Description:        "Cluster member store route should have no specific middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/cluster/members/{address}",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Cluster member destroy route should have Internal middleware",
		},
		{
			Method:             "POST",
			Path:               "/cluster/primary",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Cluster primary route should have Internal middleware",
		},
		{
			Method:             "POST",
			Path:               "/events",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Event store route should have Internal middleware",
		},
		{
			Method:             "GET",
			Path:               "/health",
			ExpectedMiddleware: []string{"Internal"},
			Description:        "Health check route should have Internal middleware",
		},
		// Database routes
		{
			Method:             "POST",
			Path:               "/{databaseKey}/backups",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Database backup store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/{databaseKey}/backups/{timestamp}",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Database backup show route should have Authentication middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/{databaseKey}/backups/{timestamp}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Database backup destroy route should have Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/{databaseKey}/metrics/query",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Query log route should have Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/{databaseKey}/query",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Query route should have Authentication and NodeTick middleware",
		},
		{
			Method:             "POST",
			Path:               "/{databaseKey}/query/stream",
			ExpectedMiddleware: []string{"PreloadDatabaseKey", "Authentication"},
			Description:        "Query stream route should have PreloadDatabaseKey, Authentication and NodeTick middleware",
		},
		{
			Method:             "POST",
			Path:               "/{databaseKey}/restore",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Database restore route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/{databaseKey}/snapshots",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Database snapshot index route should have Authentication middleware",
		},
		{
			Method:             "GET",
			Path:               "/{databaseKey}/snapshots/{timestamp}",
			ExpectedMiddleware: []string{"Authentication"},
			Description:        "Database snapshot show route should have Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/{databaseKey}/transactions",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Transaction store route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "DELETE",
			Path:               "/{databaseKey}/transactions/{id}",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Transaction destroy route should have ForwardToPrimary and Authentication middleware",
		},
		{
			Method:             "POST",
			Path:               "/{databaseKey}/transactions/{id}/commit",
			ExpectedMiddleware: []string{"ForwardToPrimary", "Authentication"},
			Description:        "Transaction commit route should have ForwardToPrimary and Authentication middleware",
		},
	}

	// Create a router and load routes
	router := appHttp.NewRouter()
	appHttp.LoadRoutes(router)

	// Test each route
	for _, test := range routeTests {
		t.Run(test.Description, func(t *testing.T) {
			// Find the route in the router
			methodRoutes, exists := router.Routes[test.Method]
			if !exists {
				t.Fatalf("Method %s not found in router", test.Method)
			}

			var foundRoute *appHttp.Route

			for path, route := range methodRoutes {
				if path == test.Path {
					foundRoute = route
					break
				}
			}

			if foundRoute == nil {
				t.Fatalf("Route %s %s not found", test.Method, test.Path)
			}

			// Get the actual middleware names
			actualMiddleware := convertMiddlewareSliceToNames(foundRoute.RegisteredMiddleware)

			// Compare expected vs actual middleware
			if !reflect.DeepEqual(test.ExpectedMiddleware, actualMiddleware) {
				t.Errorf("Route %s %s middleware mismatch.\nExpected: %v\nActual: %v",
					test.Method, test.Path, test.ExpectedMiddleware, actualMiddleware)
			}
		})
	}
}

// TestAllRoutesHaveMiddleware tests that we haven't missed any routes in our test cases
func TestAllRoutesHaveMiddleware(t *testing.T) {
	router := appHttp.NewRouter()
	appHttp.LoadRoutes(router)

	// Count total routes defined in our test cases
	expectedRouteCount := 36 // Update this number if you add more routes

	totalRoutes := 0
	for method, methodRoutes := range router.Routes {
		totalRoutes += len(methodRoutes)
		t.Logf("Method %s has %d routes", method, len(methodRoutes))
	}

	if totalRoutes != expectedRouteCount {
		t.Errorf(
			"Expected %d routes in test cases, but found %d total routes in router. Please update test cases.",
			expectedRouteCount, totalRoutes,
		)

		// List all routes for debugging
		for method, methodRoutes := range router.Routes {
			for path := range methodRoutes {
				t.Logf("Found route: %s %s", method, path)
			}
		}
	}
}

// TestMiddlewareIdentification tests that our middleware identification works correctly
func TestMiddlewareIdentification(t *testing.T) {
	testCases := []struct {
		middleware appHttp.Middleware
		expected   string
	}{
		{appHttp.RequireHost, "RequireHost"},
		{appHttp.Authentication, "Authentication"},
		{appHttp.ForwardToPrimary, "ForwardToPrimary"},
		{appHttp.Internal, "Internal"},
		{appHttp.NodeTick, "NodeTick"},
		{appHttp.PreloadDatabaseKey, "PreloadDatabaseKey"},
	}

	for _, test := range testCases {
		actual := middlewareToString(test.middleware)

		if actual != test.expected {
			t.Errorf("Expected middleware name %s, got %s", test.expected, actual)
		}
	}
}
