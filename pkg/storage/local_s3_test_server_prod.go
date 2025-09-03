//go:build production

package storage

import "net/http/httptest"

// s3Server is nil in production builds since we don't use the test server
var s3Server *httptest.Server
