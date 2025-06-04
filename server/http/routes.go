package http

import (
	"time"
)

func LoadRoutes(router *Router) {
	// Administrative routes.
	router.Get(
		"/cluster/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/users/",
		UserControllerIndex,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/users",
		UserControllerStore,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Delete(
		"/users/{username}",
		UserControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/access-keys/",
		AccessKeyControllerIndex,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Delete(
		"/access-keys/{accessKeyId}",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/access-keys/purge",
		AccessKeyPurgeController,
	).Middleware([]Middleware{
		Internal,
	})

	router.Get(
		"/databases/",
		DatabaseIndexController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/databases/{database_id}",
		DatabaseShowController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/databases",
		DatabaseStoreController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Delete(
		"/databases/{databaseId}",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/signature",
		SingatureStoreController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/signature/activate",
		SingatureActivateController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	// Internal routes for cluster operations.
	router.Post(
		"/cluster/connection",
		ClusterConnectionController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/cluster/election",
		ClusterElectionController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(3 * time.Second)

	router.Post(
		"/cluster/members",
		ClusterMemberStoreController,
	).Middleware(
		[]Middleware{
			// TODO: PrivateNetwork,
		},
	).Timeout(3 * time.Second)

	router.Delete(
		"/cluster/members/{address}",
		ClusterMemberDestroyController,
	).Middleware(
		[]Middleware{
			// TODO: PrivateNetwork,
		},
	).Timeout(3 * time.Second)

	router.Post(
		"/cluster/primary",
		ClusterPrimaryController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/databases/{databaseId}/{branchId}/settings/purge",
		DatabaseSettingsPurgeController,
	).Middleware([]Middleware{
		Internal,
	})

	router.Post(
		"/events",
		EventStoreController,
	).Middleware([]Middleware{
		Internal,
	})

	router.Get(
		"/health",
		HealthCheckController,
	).Middleware([]Middleware{
		Internal,
	})

	// Database routes.
	router.Post("/backups",
		DatabaseBackupStoreController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Get("/backups/{timestamp}",
		DatabaseBackupShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Get("/metrics/query",
		QueryLogController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		NodeTick,
	}).Timeout(1 * time.Second)

	router.Get("/ping",
		PingController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Post("/query",
		QueryController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/query/stream",
		QueryStreamController,
	).Middleware([]Middleware{
		RequireSubdomain,
		PreloadDatabaseKey,
		Authentication,
		Authorization,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/restore",
		DatabaseRestoreController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Get("/snapshots/",
		DatabaseSnapshotIndexController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Get("/snapshots/{timestamp}",
		DatabaseSnapshotShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Post("/transactions",
		TransactionControllerStore,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Delete("/transactions/{id}",
		TransactionControllerDestroy,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Post("/transactions/{id}/commit",
		TransactionCommitController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Fallback(func(request *Request) Response {
		return Response{
			StatusCode: 404,
			Body:       nil,
		}
	})
}
