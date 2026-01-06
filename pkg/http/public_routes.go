package http

import (
	"context"
	"time"
)

func LoadPublicRoutes(router *Router) {
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

	router.Patch(
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

	router.Patch(
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

	router.Patch(
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
		"/v1/databases/{databaseName}/branches/{branchName}",
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
		"/v1/databases/{databaseName}/branches/{branchName}",
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
		"/v1/databases/{databaseName}/branches/{branchName}/export",
		DatabaseExportControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/databases/{databaseName}/branches/{branchName}/export/{exportId}/ranges/{rangeNumber}",
		DatabaseExportPartControllerShow,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/databases/{databaseName}/branches/{branchName}/export/{exportId}/end",
		DatabaseExportEndControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/imports",
		ImportControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get(
		"/v1/imports/{importId}",
		ImportControllerShow,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Patch(
		"/v1/imports/{importId}",
		ImportControllerUpdate,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Delete(
		"/v1/imports/{importId}",
		ImportControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post(
		"/v1/imports/{importId}/chunks",
		ImportChunkControllerStore,
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

	router.Get("/v1/databases/{databaseName}/branches/{branchName}/backups",
		DatabaseBackupControllerIndex,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Post("/v1/databases/{databaseName}/branches/{branchName}/backups",
		DatabaseBackupControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/branches/{branchName}/backups/{timestamp}",
		DatabaseBackupControllerShow,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Delete("/v1/databases/{databaseName}/branches/{branchName}/backups/{timestamp}",
		DatabaseBackupControllerDestroy,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/branches/{branchName}/metrics/query",
		QueryLogControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(1 * time.Second)

	router.Post("/v1/databases/{databaseName}/branches/{branchName}/query",
		QueryControllerStore,
	).Middleware([]Middleware{
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/branches/{branchName}/query/stream",
		QueryStreamControllerStore,
	).Middleware([]Middleware{
		PreloadDatabaseKey,
		Authentication,
	}).Timeout(300 * time.Second)

	router.Post("/v1/databases/{databaseName}/branches/{branchName}/restore",
		DatabaseRestoreControllerStore,
	).Middleware([]Middleware{
		ForwardToPrimary,
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/branches/{branchName}/snapshots",
		DatabaseSnapshotControllerIndex,
	).Middleware([]Middleware{
		Authentication,
	})

	router.Get("/v1/databases/{databaseName}/branches/{branchName}/snapshots/{timestamp}",
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
