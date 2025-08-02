package http

import (
	"time"
)

func LoadRoutes(router *Router) {
	// Administrative routes
	router.Get(
		"/v1/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		RequireHost,
		Authentication,
	})

	router.Get(
		"/v1/users",
		UserControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/users/{username}",
		UserControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/users",
		UserControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/users/{username}",
		UserControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/v1/users/{username}",
		UserControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/access-keys",
		AccessKeyControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/access-keys/{accessKeyId}",
		AccessKeyControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/v1/access-keys/{accessKeyId}",
		AccessKeyControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/access-keys/{accessKeyId}",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}/branches",
		DatabaseBranchIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}/{branchName}",
		DatabaseBranchShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/databases/{databaseName}/branches",
		DatabaseBranchStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/databases/{databaseName}/{branchName}",
		DatabaseBranchDestroyController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/databases",
		DatabaseIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}",
		DatabaseShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/databases",
		DatabaseStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/databases/{databaseName}",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/keys",
		KeyStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/keys/activate",
		KeyActivateController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	// Internal routes for cluster operations.
	router.Post(
		"/v1/cluster/connection",
		ClusterConnectionController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/v1/cluster/election",
		ClusterElectionController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(3 * time.Second)

	router.Post(
		"/v1/cluster/members",
		ClusterMemberStoreController,
	).Middleware(
		[]Middleware{},
	).Timeout(3 * time.Second)

	router.Delete(
		"/v1/cluster/members/{address}",
		ClusterMemberDestroyController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(3 * time.Second)

	router.Post(
		"/v1/cluster/primary",
		ClusterPrimaryController,
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
		HealthCheckController,
	).Middleware([]Middleware{
		Internal,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/backups",
		DatabaseBackupIndexController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post("/v1/databases/{databaseName}/{branchName}/backups",
		DatabaseBackupStoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/backups/{timestamp}",
		DatabaseBackupShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete("/v1/databases/{databaseName}/{branchName}/backups/{timestamp}",
		DatabaseBackupDestroyController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/metrics/query",
		QueryLogController,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(1 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/query",
		QueryController,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/query/stream",
		QueryStreamController,
	).Middleware([]Middleware{
		PreloadDatabaseKey,
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/restore",
		DatabaseRestoreController,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/snapshots",
		DatabaseSnapshotIndexController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/snapshots/{timestamp}",
		DatabaseSnapshotShowController,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post("/v1/databases/{databaseName}/{branchName}/transactions",
		TransactionControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete("/v1/databases/{databaseName}/{branchName}/transactions/{id}",
		TransactionControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post("/v1/databases/{databaseName}/{branchName}/transactions/{id}/commit",
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
