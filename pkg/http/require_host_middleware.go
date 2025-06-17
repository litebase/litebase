package http

import (
	"strings"
)

// Protect server from host header attacks by matching the configured hostname.
func RequireHost(request *Request) (*Request, Response) {
	hostname := request.cluster.Config.HostName
	hostHeader := request.BaseRequest.Host

	// Validate Host header format
	if hostHeader == "" {
		return request, Response{
			StatusCode: 400,
			Body: map[string]any{
				"status":  "error",
				"message": "Missing Host header",
			},
		}
	}

	// Extract hostname from Host header (remove port if present)
	actualHost := strings.Split(hostHeader, ":")[0]

	// Special case for localhost
	if hostname == "localhost" && (actualHost == "localhost" || actualHost == "127.0.0.1") {
		return request, Response{}
	}

	if actualHost != hostname {
		return request, Response{
			StatusCode: 403,
			Body: map[string]any{
				"status":  "error",
				"message": "Forbidden",
			},
		}
	}

	return request, Response{}
}
