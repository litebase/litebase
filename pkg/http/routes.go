package http

import (
	"context"
	"time"
)

func LoadRoutes(router *Router) {
	// Administrative routes
	router.Get(
		"/v1/status",
		ClusterStatusControllerIndex,
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
		"/v1/tokens",
		TokenControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/tokens/{tokenId}",
		TokenControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/tokens",
		TokenControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Put(
		"/v1/tokens/{tokenId}",
		TokenControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/tokens/{tokenId}",
		TokenControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}/branches",
		DatabaseBranchControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}/{branchName}",
		DatabaseBranchControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/databases/{databaseName}/branches",
		DatabaseBranchControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/databases/{databaseName}/{branchName}",
		DatabaseBranchControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/databases",
		DatabaseControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}",
		DatabaseControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Post(
		"/v1/databases",
		DatabaseControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/databases/{databaseName}",
		DatabaseControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/keys",
		KeyControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/keys/activate",
		KeyActivateControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

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

	router.Get("/v1/databases/{databaseName}/{branchName}/backups",
		DatabaseBackupControllerIndex,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post("/v1/databases/{databaseName}/{branchName}/backups",
		DatabaseBackupControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/backups/{timestamp}",
		DatabaseBackupControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete("/v1/databases/{databaseName}/{branchName}/backups/{timestamp}",
		DatabaseBackupControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/metrics/query",
		QueryLogControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(1 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/query",
		QueryControllerStore,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/query/stream",
		QueryStreamControllerStore,
	).Middleware([]Middleware{
		PreloadDatabaseKey,
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/{branchName}/restore",
		DatabaseRestoreControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/snapshots",
		DatabaseSnapshotControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/{branchName}/snapshots/{timestamp}",
		DatabaseSnapshotControllerShow,
	).Middleware([]Middleware{
		Authentication,
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
