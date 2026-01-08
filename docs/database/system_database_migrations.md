# System Database Migrations

This directory contains migrations for the system database. Migrations are applied automatically on startup when the system database is initialized.

## Migration Structure

Each migration is a Go file that implements a migration function with the following signature:

```go
func MigrationXXX_description(db *sql.DB) error
```

## Creating a New Migration

1. Create a new file in this directory with a naming pattern like `002_add_new_table.go`
2. Implement your migration function:

    ```go
    package migrations

    import "database/sql"

    // Migration002AddNewTable adds a new table to the system database.
    func Migration002AddNewTable(db *sql.DB) error {
        _, err := db.Exec(
            `CREATE TABLE IF NOT EXISTS new_table
            (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );`,
        )

        return err
    }
    ```

3. Register your migration in `migrations.go`:

    ```go
    var AllMigrations = []Migration{
        {
            Name: "001_initial_schema",
            Up:   Migration001InitialSchema,
        },
        {
            Name: "002_add_new_table",
            Up:   Migration002AddNewTable,
        },
        // Add new migrations here
    }
    ```

## Migration Naming

- Migration names should be unique and sortable lexicographically
- Use a numeric prefix (001, 002, etc.) to ensure proper ordering
- Use descriptive names that clearly indicate what the migration does
- Example: `001_initial_schema`, `002_add_users_table`, `003_add_index_on_users`

## Important Notes

- Migrations are added to the `AllMigrations` variable in `migrations.go`
- The `GetAllMigrations()` function returns the `AllMigrations` variable
- Migrations are executed in the order they appear in the `AllMigrations` slice
- Each migration is tracked in the `migrations` table in the system database
- Migrations that have already been applied will be skipped on subsequent runs
- Always use `CREATE TABLE IF NOT EXISTS` and similar idempotent patterns
- Each migration runs in a transaction, so failures will rollback
- The migrations table is created before any other tables to track migration history
- In tests, `AllMigrations` can be modified to simulate new deployments
