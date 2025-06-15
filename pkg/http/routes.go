package http

import (
	"log"
	"time"
)

func LoadRoutes(router *Router) {
	// Administrative routes
	router.Get(
		"/cluster/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/users",
		UserControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/users",
		UserControllerStore,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete(
		"/users/{username}",
		UserControllerDestroy,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/access-keys",
		AccessKeyControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/access-keys/{accessKeyId}",
		AccessKeyControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/access-keys/{accessKeyId}",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/databases",
		DatabaseIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/databases/{databaseId}",
		DatabaseShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/databases",
		DatabaseStoreController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete(
		"/databases/{databaseId}",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/signature",
		SingatureStoreController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/signature/activate",
		SingatureActivateController,
	).Middleware([]Middleware{
		Authentication,
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
		[]Middleware{},
	).Timeout(3 * time.Second)

	router.Delete(
		"/cluster/members/{address}",
		ClusterMemberDestroyController,
	).Middleware(
		[]Middleware{Internal},
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
	})

	router.Get("/backups/{timestamp}",
		DatabaseBackupShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Delete("/backups/{timestamp}",
		DatabaseBackupDestroyController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Get("/metrics/query",
		QueryLogController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		NodeTick,
	}).Timeout(1 * time.Second)

	router.Post("/query",
		QueryController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/query/stream",
		QueryStreamController,
	).Middleware([]Middleware{
		RequireSubdomain,
		PreloadDatabaseKey,
		Authentication,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/restore",
		DatabaseRestoreController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Get("/snapshots",
		DatabaseSnapshotIndexController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Get("/snapshots/{timestamp}",
		DatabaseSnapshotShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Post("/transactions",
		TransactionControllerStore,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Delete("/transactions/{id}",
		TransactionControllerDestroy,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Post("/transactions/{id}/commit",
		TransactionCommitController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
	})

	router.Fallback(func(request *Request) Response {
		log.Println("404")
		return Response{
			StatusCode: 404,
			Body: map[string]any{
				"status": "error",
			},
		}
	})
}
