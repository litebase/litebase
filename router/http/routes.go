package http

import (
	"fmt"
	"litebasedb/router/config"

	"github.com/gofiber/fiber/v2"
)

func Routes(router *fiber.App) {
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
		RequireHost(host),
		AdminAuth,
		AccessKeyControllerStore,
	)

	router.Delete(
		"/databases/:uuid/:branchUuid/access-keys/:accessKeyId",
		RequireHost(host),
		AdminAuth,
		AccessKeyControllerDestroy,
	)

	router.Post(
		"/databases/:uuid/:branchUuid/settings",
		RequireHost(host),
		AdminAuth,
		DatabaseSettingsStoreController,
	)

	router.Delete(
		"/databases/:uuid/:branchUuid/settings",
		RequireHost(host),
		AdminAuth,
		DatabaseSettingsDestroyController,
	)

	router.Post(
		"/signature",
		RequireHost(host),
		AdminAuth,
		SingatureStoreController,
	)

	/**
	 * Internal routes for cluster operations.
	 */
	router.Post(
		"/databases/:uuid/:branchUuid/settings/purge",
		Internal,
		DatabaseSettingsPurgeController,
	)

	router.Post(
		"/databases/:uuid/:branchUuid/access-keys/purge",
		Internal,
		AccessKeyPurgeController,
	)

	/**
	 * Runtime routes.
	 */
	router.Get(
		"/databases/:uuid/:branchUuid/connection",
		RuntimeAuth,
		ConnectionController,
	)

	/**
	 * Database routes.
	 */
	router.Post("/query",
		RequireSubdomain,
		Auth,
		QueryController,
	)

	router.Post("/transactions", RequireSubdomain,
		Auth,
		TrasactionControllerStore,
	)

	router.Delete("/transactions/:id/", RequireSubdomain,
		Auth,
		TrasactionControllerDestroy,
	)

	router.Post("/transactions/:id/", RequireSubdomain,
		Auth,
		TrasactionControllerUpdate,
	)

	router.Post("/transactions/:id/commit", RequireSubdomain,
		Auth,
		TransactionCommitController,
	)
}
