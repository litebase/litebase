package http

import (
	"time"
)

func LoadRoutes(router *RouterInstance) {
	// Administrative routes.
	router.Get(
		"/cluster/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Get(
		"/users/",
		UserControllerIndex,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/users",
		UserControllerStore,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Delete(
		"/users/{username}",
		UserControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Get(
		"/access-keys/",
		AccessKeyControllerIndex,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Delete(
		"/access-keys/{accessKeyId}",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/access-keys/purge",
		AccessKeyPurgeController,
	).Middleware([]Middleware{
		Internal,
		QueryNode,
	})

	router.Get(
		"/databases/",
		DatabaseIndexController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Get(
		"/databases/{database_id}",
		DatabaseShowController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/databases",
		DatabaseStoreController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Delete(
		"/databases/{databaseId}",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/databases/{databaseId}/public-key",
		DatabasePublicKeyController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/signature",
		SingatureStoreController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
	})

	router.Post(
		"/signature/activate",
		SingatureActivateController,
	).Middleware([]Middleware{
		AdminAuth,
		QueryNode,
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
	).Timeout(0)

	router.Post(
		"/cluster/election/confirmation",
		ClusterElectionConfirmationController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

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
		"/cluster/replica",
		ClusterReplicaController,
	).Middleware(
		[]Middleware{Internal},
	).Timeout(0)

	router.Post(
		"/databases/{databaseId}/{branchId}/settings/purge",
		DatabaseSettingsPurgeController,
	).Middleware([]Middleware{
		Internal,
		QueryNode,
	})

	router.Post(
		"/events",
		EventStoreController,
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
		QueryNode,
	})

	router.Get("/backups/{timestamp}",
		DatabaseBackupShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Get("/metrics/query",
		QueryLogController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
		NodeTick,
	}).Timeout(1 * time.Second)

	router.Post("/query",
		QueryController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/query/stream",
		QueryStreamController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
		NodeTick,
	}).Timeout(300 * time.Second)

	router.Post("/restore",
		DatabaseRestoreController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Get("/snapshots/",
		DatabaseSnapshotIndexController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Get("/snapshots/{timestamp}",
		DatabaseSnapshotShowController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Post("/storage",
		DistributedStorageController,
	).Middleware([]Middleware{
		Internal,
		StorageNode,
	})

	router.Post("/transactions",
		TrasactionControllerStore,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Delete("/transactions/{id}/",
		TrasactionControllerDestroy,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Post("/transactions/{id}/",
		TrasactionControllerUpdate,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Post("/transactions/{id}/commit",
		TransactionCommitController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
		QueryNode,
	})

	router.Fallback(func(request *Request) Response {
		return Response{
			StatusCode: 404,
			Body:       nil,
		}
	})

	// TODO: Implement router.Error() for 500 errors.
}
