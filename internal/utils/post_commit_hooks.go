package utils

import "sync"

// PostCommitHook is a function executed after a successful transaction commit.
// The conn parameter is the Go-level connection wrapper (e.g.
// *database.DatabaseConnection) passed as `any` to avoid import cycles.
type PostCommitHook func(conn any)

// postCommitHooksMu protects the global hooks map.
var postCommitHooksMu sync.Mutex

// postCommitHooks maps a sqlite3* pointer (as uintptr) to a list of hooks
// registered during C-level xCommit callbacks. Hooks are drained and
// executed by the Go transaction/exec wrapper after the SQLite commit
// completes and barriers are released, so the page cache is still warm
// from the insert transaction.
var postCommitHooks = make(map[uintptr][]PostCommitHook)

// RegisterPostCommitHook adds a hook to be executed after the current
// transaction commits on the given sqlite3 connection. The dbPtr should
// be the uintptr of the C sqlite3* pointer (obtainable via
// Connection.DBPointer()).
//
// This is called from C-level virtual table callbacks (e.g.
// goTriggerClusterSplits in xCommit) to schedule work on the same
// warm-cache connection instead of firing a background goroutine.
func RegisterPostCommitHook(dbPtr uintptr, hook PostCommitHook) {
	postCommitHooksMu.Lock()
	defer postCommitHooksMu.Unlock()

	postCommitHooks[dbPtr] = append(postCommitHooks[dbPtr], hook)
}

// DrainPostCommitHooks removes and returns all hooks registered for the
// given sqlite3 connection pointer. Returns nil if none are pending.
func DrainPostCommitHooks(dbPtr uintptr) []PostCommitHook {
	postCommitHooksMu.Lock()
	defer postCommitHooksMu.Unlock()

	hooks := postCommitHooks[dbPtr]

	if len(hooks) > 0 {
		delete(postCommitHooks, dbPtr)
	}

	return hooks
}
