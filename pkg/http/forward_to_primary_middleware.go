package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func ForwardToPrimary(ctx context.Context, request *Request) (*Request, Response) {
	if request.cluster.Node().IsPrimary() {
		return request, Response{}
	}

	// Get the primary node's public address (for API requests)
	primaryAddress := request.cluster.Node().PrimaryPublicAddress()

	if primaryAddress == "" {
		return request, Response{
			StatusCode: 503,
			Body: map[string]any{
				"status":  "error",
				"message": "Primary node not available",
			},
		}
	}

	// Continue if the primary address is the same as the current node's public address
	if address, _ := request.cluster.Node().PublicAddress(); primaryAddress == address {
		return request, Response{}
	}

	// Parse the primary URL
	primaryURL, err := url.Parse(fmt.Sprintf("http://%s", primaryAddress))

	if err != nil {
		return request, Response{
			StatusCode: 500,
			Body: map[string]any{
				"status":  "error",
				"message": "Invalid primary node address",
			},
		}
	}

	// Create and configure the reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(primaryURL)

	// Return a streaming response that proxies to the primary
	return request, Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			// Use the reverse proxy to handle the request
			proxy.ServeHTTP(w, request.BaseRequest)
		},
	}
}
