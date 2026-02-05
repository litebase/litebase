package http

import (
	"context"
	"time"
)

func LoadPrivateRoutes(router *Router) {
	// Internal routes for cluster operations.
	router.Post(
		"/v1/cluster/connection",
		ClusterConnectionControllerStore,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/v1/cluster/election",
		ClusterElectionControllerStore,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(3 * time.Second)

	router.Post(
		"/v1/cluster/members",
		ClusterMemberControllerStore,
	).Middleware(
		[]Middleware{},
	).Timeout(3 * time.Second)

	router.Delete(
		"/v1/cluster/members/{address}",
		ClusterMemberControllerDestroy,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(3 * time.Second)

	router.Post(
		"/v1/cluster/primary",
		ClusterPrimaryControllerStore,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/v1/events",
		EventStoreController,
	).Middleware([]Middleware{
		Internal,
	})

	router.Get(
		"/v1/health",
		HealthCheckControllerShow,
	).Middleware([]Middleware{
		Internal,
	})

	router.Fallback(func(ctx context.Context, request *Request) Response {
		return Response{
			StatusCode: 404,
			Body: map[string]any{
				"status": "error",
			},
		}
	})
}
