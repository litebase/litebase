# Litebase Codebase Instructions for AI Agents

Litebase is a distributed relational database built on SQLite with horizontal scaling, tiered storage (local, network, object), and cluster-based primary-replica architecture.

## Architecture Overview

### Core Components

- **Server** ([pkg/server](../pkg/server/)): Entry point that coordinates cluster, database management, and routing
  - `server.NewServer()` creates both public and private HTTP servers
  - `server.App` is the main application instance holding references to cluster, auth, database manager
  - Always use `StartWithPrivateRouting()` to properly initialize both servers with port providers

- **Cluster** ([pkg/cluster](../pkg/cluster/)): Manages distributed node coordination via lease-based primary election
  - Nodes communicate via private routes on separate port (separate from public API)
  - Primary node handles writes; replicas handle reads with eventual consistency
  - Cluster state stored in network filesystem for coordination
  - Election uses distributed lease files on shared storage

- **Database** ([pkg/database](../pkg/database/)): SQLite database lifecycle and branching
  - Each database has a primary branch; branches can be created from any branch
  - Branch-level connection pooling and WAL synchronization
  - System database (`_system`) stores cluster metadata, migrations run on primary node startup

- **Storage Tiers** ([pkg/storage](../pkg/storage/), [pkg/cluster/node_file_system.go](../pkg/cluster/node_file_system.go)):
  - **LocalFS**: Instance-local storage (fast, ephemeral)
  - **NetworkFS**: Shared filesystem (EFS) for cluster coordination
  - **ObjectFS**: S3-compatible object storage for durability
  - **TieredFS**: Transparent caching layer (`NetworkFS` → `ObjectFS`)
  - **TmpTieredFS**: Local cache layer (`LocalFS` → `ObjectFS`)
  - Dirty files tracked in `/_fslogs/tiered-files`; only primary node syncs to object storage

## Development Workflows

### Running Tests

**Critical flags to use:**

- Use `-timeout` flag (not `timeout` command) for test timeouts: `go test -timeout 5m`
- Use `-count` flag for repeated runs (avoids cache): `go test -count=10`
- Use `-race` for race detection: `go test -race`

**Test utilities:**

```go
// For full server setup with cluster initialization
test.NewTestServer(t) // Returns *TestServer with started node

// For app-only tests without HTTP server
test.RunWithApp(t, func(app *server.App) { ... })

// For cleanup without starting node
test.Run(t, func() { ... })
```

All test servers automatically:

- Create isolated data directories
- Initialize public and private HTTP servers
- Start cluster node and wait for startup (5s timeout)
- Clean up resources in `defer server.Shutdown()`

### Code Style (from [.github/instructions/code.instructions.md](./instructions/code.instructions.md))

**Mandatory spacing:**

```go
// BAD - no space before control flow
a, err := b()
if err == nil && !a {
  // ...
}

// GOOD - blank line before if/for/switch
a, err := b()

if err == nil && !a {
  // ...
}
```

Apply this spacing after **all** variable declarations and before **all** control flow blocks.

### Testing Requirements (from [.github/instructions/testing.instructions.md](./instructions/testing.instructions.md))

- Write tests for all new functionality
- Update existing tests when modifying functionality
- Run package tests before confirming changes
- **Never** use `timeout` command; use `-timeout` flag
- Use `-count` flag for repeated runs, not loops

### Writing Documentation (from [.github/instructions/documentation.instructions.md](./instructions/documentation.instructions.md))

- Place in `docs/` under package-specific subdirectories
- Create `index.md` in each subdirectory linking to other files
- Create `overview.md` describing high-level architecture
- Focus on **what the code does**, not just where it is
- Include API/CLI usage examples since Litebase is API-first
- Avoid just pointing to code locations; explain functionality

## Key Patterns

### Server Initialization

```go
// cmd/litebase-server/main.go pattern
srv := server.NewServer(configInstance)
srv.StartWithPrivateRouting(
    func(publicMux *http.ServeMux, app *server.App) {
        app.Run() // Sets up public routes
        <-app.Cluster.Node().Start() // Wait for node startup
    },
    func(privateMux *http.ServeMux, app *server.App) {
        router := httpRouter.NewRouter()
        router.PrivateServer(app.Cluster, app.DatabaseManager, app.LogManager, privateMux)
    },
    func(app *server.App) {
        // Shutdown hooks
    },
)
```

### File System Selection

- **NetworkFS** for cluster coordination files (`_cluster/`, `_nodes/`, `_databases/`)
- **ObjectFS** for long-term durable storage (backups, archives)
- **TieredFS** for active database files (WAL, journals)
- **LocalFS** for temporary, instance-specific data

### Testing Multi-Process Scenarios

Use `test.WithSteps()` for distributed scenarios (see [pkg/cluster/cluster_election_test.go](../pkg/cluster/cluster_election_test.go)):

```go
test.WithSteps(t, func(sp *test.StepProcessor) {
    sp.Run("PRIMARY", func(s *test.StepProcess) {
        s.Step("READY")
        // ...
    })
    sp.Run("REPLICA", func(s *test.StepProcess) {
        s.WaitForStep("READY")
        // ...
    })
})
```

### Background Job System

Jobs registered via `app.QueueWorkerPool.RegisterJob()` with options:

- `queue.WithTimeout(duration)` - job execution timeout
- `queue.WithRetries(maxRetries, retryDelay)` - retry configuration
- `queue.WithQueue(name)` - queue assignment

Dispatch with `app.QueueDispatcher.DispatchJob(name, data)`

### Scheduler Tasks

Register tasks in `app.InitScheduledTasks()`:

```go
app.Scheduler.RegisterTask("TaskName",
    func(ctx context.Context) error { ... },
    scheduler.WithSchedule(scheduler.EveryMinute),
)
```

Scheduler only runs tasks on primary node (checked internally).

## Common Gotchas

1. **Don't use `python -c` or `timeout` commands in tests** - use proper flags and test helpers
2. **Always wait for node startup** - `<-app.Cluster.Node().Start()` blocks until ready
3. **TieredFS syncs only on primary** - replicas read from object storage directly
4. **System database migrations** - only run on primary node via `OnStarted()` callback
5. **Port providers must be set** - before cluster init when using test servers
6. **Private vs public routes** - cluster communication uses private server, APIs use public

## Entry Points

- **CLI**: [cmd/litebase/main.go](../cmd/litebase/main.go) → [pkg/cli/cmd/](../pkg/cli/cmd/)
- **Server**: [cmd/litebase-server/main.go](../cmd/litebase-server/main.go) → [pkg/server/app.go](../pkg/server/app.go)
- **Router**: [cmd/litebase-router/main.go](../cmd/litebase-router/main.go) → [pkg/router/router.go](../pkg/router/router.go)
- **Tests**: [internal/test/](../internal/test/) provides test helpers and utilities

## Critical Files

- [pkg/server/app.go](../pkg/server/app.go) - Application initialization and lifecycle
- [pkg/cluster/node.go](../pkg/cluster/node.go) - Node state management and primary election
- [pkg/database/database.go](../pkg/database/database.go) - Database and branch management
- [pkg/storage/tiered_file_system_driver.go](../pkg/storage/tiered_file_system_driver.go) - Tiered storage implementation
- [internal/test/setup.go](../internal/test/setup.go) - Test environment configuration
