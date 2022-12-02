package http

func LoadRoutes(router *RouterInstance) {
	router.Post("connection", (&ConnectionController{}).Store).Middleware([]Middleware{
		&ConnectionAuthMiddleware{},
	})

	router.Post("query", (&QueryController{}).Store).Middleware([]Middleware{
		&AuthMiddleware{},
	})

	router.Post("transactions", (&TransactionController{}).Store).Middleware([]Middleware{
		&AuthMiddleware{},
	})

	router.Post("transactions/:id", (&TransactionController{}).Update).Middleware([]Middleware{
		&AuthMiddleware{},
	})

	router.Delete("transactions/:id", (&TransactionController{}).Destroy).Middleware([]Middleware{
		&AuthMiddleware{},
	})

	router.Post("transactions/:id/commit", (&TransactionController{}).Store).Middleware([]Middleware{
		&AuthMiddleware{},
	})

	// Admin routes
	router.Delete("databases/:database/:branch", (&DatabaseController{}).Destroy).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/access-keys", (&AccessKeyController{}).Store).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Patch("databases/:database/:branch/access-keys/:accessKeyId", (&AccessKeyController{}).Update).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Delete("databases/:database/:branch/access-keys/:accessKeyId", (&AccessKeyController{}).Destroy).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/backup", (&DatabaseBackupController{}).Store).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Get("databases/:database/:branch/backup/:timestamp", (&DatabaseBackupController{}).Show).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Delete("databases/:database/:branch/backup/:timestamp", (&DatabaseBackupController{}).Destroy).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/backup/:timestamp/archive", (&DatabaseBackupArchiveController{}).Store).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Delete("databases/:database/:branch/backup/:timestamp/archive", (&DatabaseBackupArchiveController{}).Destroy).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/directory", (&DatabaseDirectoryController{}).Index).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Get("databases/:database/:branch/query-logs", (&QueryLogController{}).Index).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/restore", (&DatabaseRestoreController{}).Store).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Get("databases/:database/:branch/snapshots/:timestamp", (&DatabaseSnapshotController{}).Show).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("databases/:database/:branch/settings", (&DatabaseSettingsController{}).Store).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Delete("databases/:database/:branch/settings", (&DatabaseSettingsController{}).Destroy).Middleware([]Middleware{
		&AdminMiddleware{},
	})

	router.Post("signature", (&SignatureController{}).Store).Middleware([]Middleware{
		// &AdminMiddleware{},
	})

	router.Fallback(func(request *Request) *Response {
		return &Response{StatusCode: 404}
	})
}
