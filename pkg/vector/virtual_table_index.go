package vector

/*
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../sqlite3/sqlite3.h"

// Distance metric constants
#define DISTANCE_METRIC_L2 0
#define DISTANCE_METRIC_COSINE 1
#define DISTANCE_METRIC_DOT 2
#define DISTANCE_METRIC_HAMMING 3

// Context passed to vector_index virtual table (same pattern as vector_scan)
typedef struct {
    char *vfsID;
    char *databaseID;
    char *branchID;
} VectorIndexContext;

// Forward declaration for Go callback
extern void goNotifyVectorInsert(char *databaseID, char *branchID, char *tableName);

// Forward declarations for virtual table module callbacks
int vector_index_create(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr);
int vector_index_connect(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr);
int vector_index_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo);
int vector_index_disconnect(sqlite3_vtab *pVtab);
int vector_index_destroy(sqlite3_vtab *pVtab);
int vector_index_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor);
int vector_index_close(sqlite3_vtab_cursor *pCursor);
int vector_index_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv);
int vector_index_next(sqlite3_vtab_cursor *pCursor);
int vector_index_eof(sqlite3_vtab_cursor *pCursor);
int vector_index_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int i);
int vector_index_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid);
int vector_index_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid);

// Virtual table module definition
static sqlite3_module vector_index_module = {
    0,                           // iVersion
    vector_index_create,         // xCreate
    vector_index_connect,        // xConnect
    vector_index_best_index,     // xBestIndex
    vector_index_disconnect,     // xDisconnect
    vector_index_destroy,        // xDestroy
    vector_index_open,           // xOpen
    vector_index_close,          // xClose
    vector_index_filter,         // xFilter
    vector_index_next,           // xNext
    vector_index_eof,            // xEof
    vector_index_column,         // xColumn
    vector_index_rowid,          // xRowid
    vector_index_update,         // xUpdate
    NULL,                        // xBegin
    NULL,                        // xSync
    NULL,                        // xCommit
    NULL,                        // xRollback
    NULL,                        // xFindFunction
    NULL,                        // xRename
    NULL,                        // xSavepoint
    NULL,                        // xRelease
    NULL,                        // xRollbackTo
};

// Vector index virtual table structure
typedef struct vector_index_vtab {
    sqlite3_vtab base;
    sqlite3 *db;
    void *pAux;  // Connection context
    char *table_name;
    int dimensions;
    int distance_metric;
    int max_cluster_size;
    int min_cluster_size;
} vector_index_vtab;

// Vector index cursor structure
typedef struct vector_index_cursor {
    sqlite3_vtab_cursor base;
    sqlite3_stmt *stmt;
    int eof;
} vector_index_cursor;

// Create shadow tables for the vector index
static int create_shadow_tables(sqlite3 *db, const char *table_name, char **pzErr) {
    char *sql = NULL;
    char *err_msg = NULL;
    int rc;

    // Create _pending table for write-ahead pattern
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_pending ("
        "id INTEGER PRIMARY KEY,"
        "vector_blob BLOB,"  // NULL for DELETE operations
        "operation TEXT NOT NULL,"
        "created_at INTEGER NOT NULL"
        ")",
        table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create pending table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create index on created_at for fast batch fetches (ORDER BY created_at ASC)
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_pending_created_idx ON %s_pending(created_at ASC)",
        table_name, table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create pending table index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create _clusters table for cluster metadata
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_clusters ("
        "cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "centroid_blob BLOB NOT NULL,"
        "cluster_size INTEGER DEFAULT 0,"
        "radius REAL DEFAULT 0.0,"
        "is_active INTEGER DEFAULT 1,"
        "version INTEGER DEFAULT 1,"
        "indexed_up_to_timestamp INTEGER DEFAULT 0,"
        "deactivated_at INTEGER DEFAULT NULL"
        ")",
        table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create clusters table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create index on is_active for fast active cluster queries
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_clusters_active_idx ON %s_clusters(is_active) WHERE is_active = 1",
        table_name, table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create clusters index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create _indexed table for assigned vectors
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_indexed ("
        "id INTEGER NOT NULL,"
        "cluster_id INTEGER NOT NULL,"
        "cluster_version INTEGER NOT NULL,"
        "vector_blob BLOB NOT NULL,"
        "indexed_at INTEGER NOT NULL,"
        "PRIMARY KEY (id),"
        "FOREIGN KEY (cluster_id) REFERENCES %s_clusters(cluster_id)"
        ")",
        table_name, table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create indexed table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create index on cluster_id for fast cluster member queries
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_indexed_cluster_idx ON %s_indexed(cluster_id)",
        table_name, table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create indexed table index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create _stats table for incremental centroid maintenance
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_stats ("
        "cluster_id INTEGER PRIMARY KEY,"
        "sum_vector_blob BLOB,"
        "last_updated INTEGER,"
        "pending_updates INTEGER DEFAULT 0,"
        "FOREIGN KEY (cluster_id) REFERENCES %s_clusters(cluster_id)"
        ")",
        table_name, table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create stats table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create _metadata table for configuration and state
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_metadata ("
        "key TEXT PRIMARY KEY,"
        "value TEXT NOT NULL"
        ")",
        table_name
    );
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to create metadata table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    return SQLITE_OK;
}

// Parse CREATE VIRTUAL TABLE parameters
static int parse_index_params(
    int argc,
    const char *const *argv,
    int *dimensions,
    int *distance_metric,
    int *max_cluster_size,
    int *min_cluster_size,
    char **pzErr
) {
    // Default values
    *dimensions = 0;
    *distance_metric = DISTANCE_METRIC_COSINE;
    *max_cluster_size = 5000;
    *min_cluster_size = 200;

    // Parse arguments: argv[0] = module name, argv[1] = database, argv[2] = table name
    // Parameters start at argv[3]
    for (int i = 3; i < argc; i++) {
        const char *arg = argv[i];

        // Skip 'id' and 'vector' column names
        if (strcmp(arg, "id") == 0 || strcmp(arg, "vector") == 0) {
            continue;
        }

        // Parse key=value parameters
        const char *eq = strchr(arg, '=');
        if (eq != NULL) {
            size_t key_len = eq - arg;
            const char *value = eq + 1;

            if (strncmp(arg, "dimensions", key_len) == 0) {
                *dimensions = atoi(value);
            } else if (strncmp(arg, "distance_metric", key_len) == 0) {
                if (strcmp(value, "'l2'") == 0 || strcmp(value, "l2") == 0) {
                    *distance_metric = DISTANCE_METRIC_L2;
                } else if (strcmp(value, "'cosine'") == 0 || strcmp(value, "cosine") == 0) {
                    *distance_metric = DISTANCE_METRIC_COSINE;
                } else if (strcmp(value, "'dot'") == 0 || strcmp(value, "dot") == 0) {
                    *distance_metric = DISTANCE_METRIC_DOT;
                } else if (strcmp(value, "'hamming'") == 0 || strcmp(value, "hamming") == 0) {
                    *distance_metric = DISTANCE_METRIC_HAMMING;
                }
            } else if (strncmp(arg, "max_cluster_size", key_len) == 0) {
                *max_cluster_size = atoi(value);
            } else if (strncmp(arg, "min_cluster_size", key_len) == 0) {
                *min_cluster_size = atoi(value);
            }
        }
    }

    // Validate required parameters
    if (*dimensions <= 0) {
        *pzErr = sqlite3_mprintf("dimensions parameter is required and must be positive");
        return SQLITE_ERROR;
    }

    if (*max_cluster_size < *min_cluster_size) {
        *pzErr = sqlite3_mprintf("max_cluster_size must be greater than min_cluster_size");
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

// xCreate: Create a new vector index virtual table
int vector_index_create(
    sqlite3 *db,
    void *pAux,
    int argc,
    const char *const *argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
) {
    vector_index_vtab *vtab;
    int rc;
    int dimensions, distance_metric, max_cluster_size, min_cluster_size;

    // Parse parameters
    rc = parse_index_params(argc, argv, &dimensions, &distance_metric, &max_cluster_size, &min_cluster_size, pzErr);
    if (rc != SQLITE_OK) {
        return rc;
    }

    // Create shadow tables
    rc = create_shadow_tables(db, argv[2], pzErr);
    if (rc != SQLITE_OK) {
        return rc;
    }

    // Allocate virtual table structure
    vtab = (vector_index_vtab *)sqlite3_malloc(sizeof(vector_index_vtab));
    if (vtab == NULL) {
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(vector_index_vtab));

    vtab->db = db;
    vtab->pAux = pAux;  // Store connection context
    vtab->table_name = sqlite3_mprintf("%s", argv[2]);
    vtab->dimensions = dimensions;
    vtab->distance_metric = distance_metric;
    vtab->max_cluster_size = max_cluster_size;
    vtab->min_cluster_size = min_cluster_size;

    // Declare virtual table schema
    rc = sqlite3_declare_vtab(db, "CREATE TABLE x(id INTEGER PRIMARY KEY, vector BLOB)");
    if (rc != SQLITE_OK) {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Store metadata
    char *sql = sqlite3_mprintf(
        "INSERT OR REPLACE INTO %s_metadata (key, value) VALUES "
        "('dimensions', '%d'), "
        "('distance_metric', '%d'), "
        "('max_cluster_size', '%d'), "
        "('min_cluster_size', '%d'), "
        "('pending_count', '0'), "
        "('last_indexed_at', '0')",
        vtab->table_name,
        dimensions,
        distance_metric,
        max_cluster_size,
        min_cluster_size
    );
    char *err_msg = NULL;
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to store metadata: %s", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    *ppVtab = (sqlite3_vtab *)vtab;
    return SQLITE_OK;
}

// xConnect: Connect to an existing vector index virtual table
int vector_index_connect(
    sqlite3 *db,
    void *pAux,
    int argc,
    const char *const *argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
) {
    vector_index_vtab *vtab;
    int rc;

    // Allocate virtual table structure
    vtab = (vector_index_vtab *)sqlite3_malloc(sizeof(vector_index_vtab));
    if (vtab == NULL) {
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(vector_index_vtab));

    vtab->db = db;
    vtab->table_name = sqlite3_mprintf("%s", argv[2]);

    // Load metadata
    char *sql = sqlite3_mprintf(
        "SELECT value FROM %s_metadata WHERE key = 'dimensions' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'distance_metric' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'max_cluster_size' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'min_cluster_size'",
        vtab->table_name, vtab->table_name, vtab->table_name, vtab->table_name
    );

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    int row = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *value = (const char *)sqlite3_column_text(stmt, 0);
        switch (row) {
            case 0: vtab->dimensions = atoi(value); break;
            case 1: vtab->distance_metric = atoi(value); break;
            case 2: vtab->max_cluster_size = atoi(value); break;
            case 3: vtab->min_cluster_size = atoi(value); break;
        }
        row++;
    }
    sqlite3_finalize(stmt);

    // Declare virtual table schema
    rc = sqlite3_declare_vtab(db, "CREATE TABLE x(id INTEGER PRIMARY KEY, vector BLOB)");
    if (rc != SQLITE_OK) {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    *ppVtab = (sqlite3_vtab *)vtab;
    return SQLITE_OK;
}

// xBestIndex: Query planner
int vector_index_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo) {
    pIdxInfo->estimatedCost = 1000.0;
    pIdxInfo->estimatedRows = 1000;
    return SQLITE_OK;
}

// xDisconnect: Disconnect from virtual table
int vector_index_disconnect(sqlite3_vtab *pVtab) {
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    sqlite3_free(vtab->table_name);
    sqlite3_free(vtab);
    return SQLITE_OK;
}

// xDestroy: Drop the virtual table and shadow tables
int vector_index_destroy(sqlite3_vtab *pVtab) {
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    char *sql;
    char *err_msg = NULL;

    // Drop shadow tables
    sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s_pending", vtab->table_name);
    sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    sqlite3_free(err_msg);

    sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s_clusters", vtab->table_name);
    sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    sqlite3_free(err_msg);

    sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s_indexed", vtab->table_name);
    sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    sqlite3_free(err_msg);

    sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s_stats", vtab->table_name);
    sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    sqlite3_free(err_msg);

    sql = sqlite3_mprintf("DROP TABLE IF EXISTS %s_metadata", vtab->table_name);
    sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    sqlite3_free(err_msg);

    // Free virtual table
    return vector_index_disconnect(pVtab);
}

// xOpen: Open a cursor
int vector_index_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
    vector_index_cursor *cursor = (vector_index_cursor *)sqlite3_malloc(sizeof(vector_index_cursor));
    if (cursor == NULL) {
        return SQLITE_NOMEM;
    }
    memset(cursor, 0, sizeof(vector_index_cursor));
    cursor->eof = 1;
    *ppCursor = (sqlite3_vtab_cursor *)cursor;
    return SQLITE_OK;
}

// xClose: Close a cursor
int vector_index_close(sqlite3_vtab_cursor *pCursor) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    if (cursor->stmt) {
        sqlite3_finalize(cursor->stmt);
    }
    sqlite3_free(cursor);
    return SQLITE_OK;
}

// xFilter: Initialize cursor for iteration
int vector_index_filter(
    sqlite3_vtab_cursor *pCursor,
    int idxNum,
    const char *idxStr,
    int argc,
    sqlite3_value **argv
) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    vector_index_vtab *vtab = (vector_index_vtab *)pCursor->pVtab;

    // Query both indexed and pending vectors
    char *sql = sqlite3_mprintf(
        "SELECT id, vector_blob FROM %s_indexed "
        "UNION ALL "
        "SELECT id, vector_blob FROM %s_pending WHERE operation = 'INSERT'",
        vtab->table_name, vtab->table_name
    );

    int rc = sqlite3_prepare_v2(vtab->db, sql, -1, &cursor->stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        return rc;
    }

    cursor->eof = 0;
    return vector_index_next(pCursor);
}

// xNext: Advance cursor
int vector_index_next(sqlite3_vtab_cursor *pCursor) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    int rc = sqlite3_step(cursor->stmt);
    if (rc == SQLITE_ROW) {
        cursor->eof = 0;
        return SQLITE_OK;
    }
    cursor->eof = 1;
    return SQLITE_OK;
}

// xEof: Check if cursor is at end
int vector_index_eof(sqlite3_vtab_cursor *pCursor) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    return cursor->eof;
}

// xColumn: Return column value
int vector_index_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int i) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    sqlite3_result_value(ctx, sqlite3_column_value(cursor->stmt, i));
    return SQLITE_OK;
}

// xRowid: Return rowid
int vector_index_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    *pRowid = sqlite3_column_int64(cursor->stmt, 0);
    return SQLITE_OK;
}

// xUpdate: Handle INSERT/UPDATE/DELETE operations
int vector_index_update(
    sqlite3_vtab *pVtab,
    int argc,
    sqlite3_value **argv,
    sqlite3_int64 *pRowid
) {
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    char *sql = NULL;
    char *err_msg = NULL;
    int rc;

    // Determine operation type
    if (argc == 1) {
        // DELETE: argc == 1, argv[0] = rowid
        sqlite3_int64 id = sqlite3_value_int64(argv[0]);

        sql = sqlite3_mprintf(
            "INSERT OR REPLACE INTO %s_pending (id, vector_blob, operation, created_at) "
            "VALUES (%lld, NULL, 'DELETE', %lld)",
            vtab->table_name, id, (sqlite3_int64)time(NULL)
        );
    } else if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        // INSERT: argv[0] = NULL, argv[1] = new rowid (might be NULL for auto-rowid), argv[2] = id, argv[3] = vector
        sqlite3_int64 new_rowid;
        const void *vector_data = sqlite3_value_blob(argv[3]);
        int vector_size = sqlite3_value_bytes(argv[3]);

        // If argv[1] is NULL, SQLite wants us to generate our own rowid
        // Otherwise, use the provided rowid
        if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            // Auto-generate rowid - use INSERT without specifying id
            sql = sqlite3_mprintf(
                "INSERT INTO %s_pending (vector_blob, operation, created_at) VALUES (?1, 'INSERT', ?2)",
                vtab->table_name
            );

            sqlite3_stmt *stmt;
            rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
            sqlite3_free(sql);
            if (rc != SQLITE_OK) return rc;

            sqlite3_bind_blob(stmt, 1, vector_data, vector_size, SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 2, (sqlite3_int64)time(NULL));

            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);
            if (rc != SQLITE_DONE) return rc;

            new_rowid = sqlite3_last_insert_rowid(vtab->db);
        } else {
            // Use provided rowid
            new_rowid = sqlite3_value_int64(argv[1]);

            sql = sqlite3_mprintf(
                "INSERT INTO %s_pending (id, vector_blob, operation, created_at) VALUES (?1, ?2, 'INSERT', ?3)",
                vtab->table_name
            );

            sqlite3_stmt *stmt;
            rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
            sqlite3_free(sql);
            if (rc != SQLITE_OK) return rc;

            sqlite3_bind_int64(stmt, 1, new_rowid);
            sqlite3_bind_blob(stmt, 2, vector_data, vector_size, SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 3, (sqlite3_int64)time(NULL));

            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);
            if (rc != SQLITE_DONE) return rc;
        }

        *pRowid = new_rowid;
        sql = NULL; // Prevent double execution
    } else if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
        // UPDATE: argv[0] = old rowid, argv[1] = new rowid, argv[2+] = columns
        sqlite3_int64 old_id = sqlite3_value_int64(argv[0]);
        sqlite3_int64 new_id = sqlite3_value_int64(argv[1]);
        const void *vector_data = sqlite3_value_blob(argv[3]);
        int vector_size = sqlite3_value_bytes(argv[3]);

        // Delete old entry (use OR REPLACE in case it's already in pending)
        sql = sqlite3_mprintf(
            "INSERT OR REPLACE INTO %s_pending (id, vector_blob, operation, created_at) "
            "VALUES (%lld, NULL, 'DELETE', %lld)",
            vtab->table_name, old_id, (sqlite3_int64)time(NULL)
        );
        rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            sqlite3_free(err_msg);
            return rc;
        }

        // Insert new entry
        sql = sqlite3_mprintf(
            "INSERT OR REPLACE INTO %s_pending (id, vector_blob, operation, created_at) VALUES (?1, ?2, 'INSERT', ?3)",
            vtab->table_name
        );

        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) return rc;

        sqlite3_bind_int64(stmt, 1, new_id);
        sqlite3_bind_blob(stmt, 2, vector_data, vector_size, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, (sqlite3_int64)time(NULL));

        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) return rc;

        sql = NULL;
    }

    // Execute SQL if not handled by prepared statement
    if (sql != NULL) {
        rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            pVtab->zErrMsg = sqlite3_mprintf("Update failed: %s", err_msg);
            sqlite3_free(err_msg);
            return rc;
        }
    }

    // Skip metadata update during transaction - batch it in xSync instead
    // This avoids updating metadata on every single insert

    // Notify VectorIndexManager about the insert
    VectorIndexContext *ctx = (VectorIndexContext *)vtab->pAux;
    if (ctx != NULL && ctx->databaseID != NULL && ctx->branchID != NULL) {
        goNotifyVectorInsert(ctx->databaseID, ctx->branchID, vtab->table_name);
    }

    return SQLITE_OK;
}

// Register vector_index module (initial registration without context)
int sqlite3_register_vector_index(sqlite3 *db) {
    return sqlite3_create_module(db, "vector_index", &vector_index_module, NULL);
}

// Register vector_index module with context for per-connection registration
int sqlite3_register_vector_index_with_context(
    sqlite3 *db,
    void *ctxPtr,
    const char *vfsID,
    const char *databaseID,
    const char *branchID
) {
    VectorIndexContext *ctx = (VectorIndexContext *)ctxPtr;
    ctx->vfsID = (char *)vfsID;
    ctx->databaseID = (char *)databaseID;
    ctx->branchID = (char *)branchID;
    return sqlite3_create_module(db, "vector_index", &vector_index_module, ctx);
}

*/
import "C"
