package http

import (
	"fmt"
	"litebasedb/router/config"
)

func LoadRoutes(router *RouterInstance) {
	host := fmt.Sprintf(
		`%s.%s`,
		config.Get("region"),
		config.Get("url"),
	)

	/**
	 * Adminstrative routes.
	 */
	router.Post(
		"/databases/:databaseUuid/:branchUuid/access-keys",
		AccessKeyControllerStore,
	).Middleware([]Middleware{
		RequireHost(host),
		AdminAuth,
	})

	router.Delete(
		"/databases/:uuid/:branchUuid/access-keys/:accessKeyId",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		RequireHost(host),
		AdminAuth,
	})

	router.Post(
		"/databases/:uuid/:branchUuid/settings",
		DatabaseSettingsStoreController,
	).Middleware([]Middleware{
		RequireHost(host),
		AdminAuth,
	})

	router.Delete(
		"/databases/:uuid/:branchUuid/settings",
		DatabaseSettingsDestroyController,
	).Middleware([]Middleware{
		RequireHost(host),
		AdminAuth,
	})

	router.Post(
		"/signature",
		SingatureStoreController,
	).Middleware([]Middleware{
		RequireHost(host),
		AdminAuth,
	})

	/**
	 * Internal routes for cluster operations.
	 */
	router.Post(
		"/databases/:uuid/:branchUuid/settings/purge",
		DatabaseSettingsPurgeController,
	).Middleware([]Middleware{Internal})

	router.Post(
		"/databases/:uuid/:branchUuid/access-keys/purge",
		AccessKeyPurgeController,
	).Middleware([]Middleware{Internal})

	/**
	 * Runtime routes.
	 */
	router.Get(
		"/databases/:uuid/:branchUuid/connection",
		ConnectionController,
	).Middleware([]Middleware{RuntimeAuth})

	/**
	 * Database routes.
	 */
	router.Post("/query",
		QueryController,
	).Middleware([]Middleware{RequireSubdomain, Auth})

	router.Post("/transactions",
		TrasactionControllerStore,
	).Middleware([]Middleware{RequireSubdomain, Auth})

	router.Delete("/transactions/:id/",
		TrasactionControllerDestroy,
	).Middleware([]Middleware{
		RequireSubdomain,
		Auth,
	})

	router.Post("/transactions/:id/",
		TrasactionControllerUpdate,
	).Middleware([]Middleware{RequireSubdomain, Auth})

	router.Post("/transactions/:id/commit",
		TransactionCommitController,
	).Middleware([]Middleware{RequireSubdomain, Auth})

	router.Fallback(func(request *Request) *Response {
		return &Response{
			StatusCode: 404,
			Body:       nil,
		}
	})

	// TODO: Implement router.Error() for 500 errors.
}
