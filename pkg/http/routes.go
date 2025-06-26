package http

import (
	"time"
)

func LoadRoutes(router *Router) {
	// Administrative routes
	router.Get(
		"/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		RequireHost,
		Authentication,
	})

	router.Get(
		"/resources/users",
		UserControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/resources/users/{username}",
		UserControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/resources/users",
		UserControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/resources/users/{username}",
		UserControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/resources/users/{username}",
		UserControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/resources/access-keys",
		AccessKeyControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/resources/access-keys/{accessKeyId}",
		AccessKeyControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/resources/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/resources/access-keys/{accessKeyId}",
		AccessKeyControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/resources/access-keys/{accessKeyId}",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/resources/databases",
		DatabaseIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/resources/databases/{databaseId}",
		DatabaseShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/resources/databases",
		DatabaseStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/resources/databases/{databaseId}",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/resources/keys",
		KeyStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/resources/keys/activate",
		KeyActivateController,
	).Middleware([]Middleware{
		ForwardToPrimary,
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
	router.Post("/{databaseKey}/backups",
		DatabaseBackupStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/{databaseKey}/backups/{timestamp}",
		DatabaseBackupShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete("/{databaseKey}/backups/{timestamp}",
		DatabaseBackupDestroyController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/{databaseKey}/metrics/query",
		QueryLogController,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(1 * time.Second)

	router.Post("/{databaseKey}/query",
		QueryController,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/{databaseKey}/query/stream",
		QueryStreamController,
	).Middleware([]Middleware{
		PreloadDatabaseKey,
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/{databaseKey}/restore",
		DatabaseRestoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/{databaseKey}/snapshots",
		DatabaseSnapshotIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get("/{databaseKey}/snapshots/{timestamp}",
		DatabaseSnapshotShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post("/{databaseKey}/transactions",
		TransactionControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete("/{databaseKey}/transactions/{id}",
		TransactionControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post("/{databaseKey}/transactions/{id}/commit",
		TransactionCommitController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Fallback(func(request *Request) Response {
		return Response{
			StatusCode: 404,
			Body: map[string]any{
				"status": "error",
			},
		}
	})
}
