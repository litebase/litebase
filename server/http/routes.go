package http

func LoadRoutes(router *RouterInstance) {
	/*
		Adminstrative routes.
	*/
	router.Get(
		"/cluster/status",
		ClusterStatusController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/users",
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
		"/users/:username",
		UserControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/access-keys",
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
		"/access-keys/:accessKeyId",
		AccessKeyControllerDestroy,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/access-keys/purge",
		AccessKeyPurgeController,
	).Middleware([]Middleware{Internal})

	router.Get(
		"/databases",
		DatabaseIndexController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Get(
		"/databases/:databaseUuid",
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
		"/databases/:databaseUuid",
		DatabaseDestroyController,
	).Middleware([]Middleware{
		AdminAuth,
	})

	router.Post(
		"/databases/:databaseUuid/public-key",
		DatabasePublicKeyController,
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

	/*
		Internal routes for cluster operations.
	*/
	router.Post(
		"/databases/:databaseUuid/:branchUuid/settings/purge",
		DatabaseSettingsPurgeController,
	).Middleware([]Middleware{Internal})

	router.Post(
		"/events",
		EventStoreController,
	).Middleware([]Middleware{Internal})

	/*
		Database routes.
	*/
	router.Post("/query",
		QueryController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Post("/transactions",
		TrasactionControllerStore,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Delete("/transactions/:id/",
		TrasactionControllerDestroy,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Post("/transactions/:id/",
		TrasactionControllerUpdate,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Post("/transactions/:id/commit",
		TransactionCommitController,
	).Middleware([]Middleware{
		RequireSubdomain,
		Authentication,
		Authorization,
	})

	router.Fallback(func(request *Request) *Response {
		return &Response{
			StatusCode: 404,
			Body:       nil,
		}
	})

	// TODO: Implement router.Error() for 500 errors.
}
