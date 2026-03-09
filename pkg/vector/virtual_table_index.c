#include "../sqlite3/sqlite3.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Distance metric constants
#define DISTANCE_METRIC_L2 0
#define DISTANCE_METRIC_COSINE 1
#define DISTANCE_METRIC_DOT 2
#define DISTANCE_METRIC_HAMMING 3

// Context passed to vector_index virtual table (same pattern as vector_scan)
typedef struct
{
    char *vfsID;
    char *databaseID;
    char *branchID;
} VectorIndexContext;

// Column definition for user-defined columns
typedef struct
{
    char *name;          // Column name
    char *type;          // Column type (INTEGER, TEXT, BLOB, REAL, etc.)
    int affinity;        // SQLite type affinity (SQLITE_INTEGER, SQLITE_TEXT, etc.)
    int is_vector;       // 1 if this is a vector column (BLOB type), 0 otherwise
    int dimensions;      // For vector columns: number of dimensions (0 for non-vector columns)
    int distance_metric; // For vector columns: distance metric (DISTANCE_METRIC_*, -1 if not set)
} ColumnDef;

// Buffer for batching inserts
typedef struct
{
    sqlite3_value **column_values; // Array of sqlite3_value* for all columns (owned, must be freed)
    int num_columns;               // Number of columns
} PendingInsert;

// SQLite has a parameter limit of 32766 (SQLITE_MAX_VARIABLE_NUMBER)
// The limiting factor is _cluster_vector_map with 3 params per row (vector_id, cluster_id, distance)
// So max batch size = 32766 / 3 = 10922 vectors
#define INSERT_BUFFER_CAPACITY 10922

// goAssignVectorsInBatch assigns each vector in the batch to its correct leaf
// cluster using the same sqlite3* connection that holds the current write
// transaction.  This eliminates cluster_id=0 entirely — no background job needed.
extern int goAssignVectorsInBatch(
    sqlite3 *db,
    char *tableName,
    char *colName,
    int distanceMetric,
    int count,
    void **blobPtrs,
    int *blobLens,
    sqlite3_int64 *clusterIDsOut,
    double *distancesOut);

// goUpdateClusterStats updates cluster_size and centroid_blob for the clusters
// that received vectors in this batch.  Same connection, same transaction.
extern int goUpdateClusterStats(
    sqlite3 *db,
    char *tableName,
    char *colName,
    int dimensions,
    int numClusters,
    sqlite3_int64 *clusterIDs,
    int *counts,
    void **vectorSumBlobs,
    int *sumBlobLens);

// goTriggerClusterSplits schedules cluster splits on the same connection as a
// post-commit hook, so the warm page cache is reused.  The dbPtr is the
// sqlite3* pointer used to correlate the C callback with the Go connection.
extern void goTriggerClusterSplits(char *databaseID, char *branchID, char *tableName, uintptr_t dbPtr);

// goFinalizeClusterStmts finalizes all cached Go-side prepared statements
// associated with the given SQLite connection.  Must be called from
// xDisconnect to avoid leaking statement objects after the connection closes.
extern void goFinalizeClusterStmts(sqlite3 *db);

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
int vector_index_begin(sqlite3_vtab *pVtab);
int vector_index_sync(sqlite3_vtab *pVtab);
int vector_index_commit(sqlite3_vtab *pVtab);
int vector_index_rollback(sqlite3_vtab *pVtab);

// Virtual table module definition
static sqlite3_module vector_index_module = {
    0,                       // iVersion
    vector_index_create,     // xCreate
    vector_index_connect,    // xConnect
    vector_index_best_index, // xBestIndex
    vector_index_disconnect, // xDisconnect
    vector_index_destroy,    // xDestroy
    vector_index_open,       // xOpen
    vector_index_close,      // xClose
    vector_index_filter,     // xFilter
    vector_index_next,       // xNext
    vector_index_eof,        // xEof
    vector_index_column,     // xColumn
    vector_index_rowid,      // xRowid
    vector_index_update,     // xUpdate
    vector_index_begin,      // xBegin
    vector_index_sync,       // xSync
    vector_index_commit,     // xCommit
    vector_index_rollback,   // xRollback
    NULL,                    // xFindFunction
    NULL,                    // xRename
    NULL,                    // xSavepoint
    NULL,                    // xRelease
    NULL,                    // xRollbackTo
};

// Vector index virtual table structure
typedef struct vector_index_vtab
{
    sqlite3_vtab base;
    sqlite3 *db;
    void *pAux; // Connection context
    char *table_name;
    int dimensions;
    int distance_metric;
    int max_cluster_size;
    int min_cluster_size;
    sqlite3_stmt *insert_vector_stmt;
    sqlite3_stmt *upsert_map_stmt;
    sqlite3_stmt *delete_map_stmt;
    sqlite3_stmt *inc_cluster_size_stmt;
    // Column definitions
    ColumnDef *columns;   // Array of column definitions
    int num_columns;      // Total number of columns (including id and vector)
    int vector_col_index; // Index of the vector column
    // Insert buffering for batched operations
    PendingInsert *insert_buffer;
    int buffer_size;     // Current number of buffered inserts
    int buffer_capacity; // Maximum buffer capacity
    int in_transaction;  // Flag to track if we're in a transaction
    // Cached prepared statements for batched operations, indexed by row count
    sqlite3_stmt **batch_vectors_stmt; // array length buffer_capacity+1
    sqlite3_stmt ***batch_map_stmt;    // [col_idx][rows] arrays, each length buffer_capacity+1
} vector_index_vtab;

// Vector index cursor structure
typedef struct vector_index_cursor
{
    sqlite3_vtab_cursor base;
    sqlite3_stmt *stmt;
    int eof;
} vector_index_cursor;

// Forward declaration for helper function (after typedef)
static int flush_insert_buffer(vector_index_vtab *vtab, char **pzErr);

// Prepare (and cache) a multi-row INSERT statement for the _vectors table
static int prepare_batch_vector_stmt(vector_index_vtab *vtab, int rows, sqlite3_stmt **pStmt, char **pzErr)
{
    if (rows <= 0 || rows > vtab->buffer_capacity)
    {
        *pzErr = sqlite3_mprintf("Invalid batch rows: %d", rows);
        return SQLITE_ERROR;
    }

    // Return cached if present
    if (vtab->batch_vectors_stmt && vtab->batch_vectors_stmt[rows] != NULL)
    {
        *pStmt = vtab->batch_vectors_stmt[rows];
        return SQLITE_OK;
    }

    // Build column list
    char *col_list = sqlite3_mprintf("");
    if (!col_list)
    {
        *pzErr = sqlite3_mprintf("Out of memory building col_list");
        return SQLITE_NOMEM;
    }
    for (int i = 0; i < vtab->num_columns; i++)
    {
        char *new_col_list = sqlite3_mprintf("%s%s%s",
                                             col_list,
                                             (i > 0 ? ", " : ""),
                                             vtab->columns[i].name);
        sqlite3_free(col_list);
        col_list = new_col_list;
        if (!col_list)
        {
            *pzErr = sqlite3_mprintf("Out of memory building col_list");
            return SQLITE_NOMEM;
        }
    }

    int params_per_row = vtab->num_columns;
    int values_size = rows * (params_per_row * 3 + 4);
    char *values_clause = (char *)sqlite3_malloc(values_size);
    if (!values_clause)
    {
        sqlite3_free(col_list);
        *pzErr = sqlite3_mprintf("Out of memory building VALUES clause");
        return SQLITE_NOMEM;
    }

    char *ptr = values_clause;
    for (int i = 0; i < rows; i++)
    {
        if (i > 0)
            ptr += sprintf(ptr, ",");
        ptr += sprintf(ptr, "(");
        for (int j = 0; j < params_per_row; j++)
        {
            if (j > 0)
                ptr += sprintf(ptr, ",");
            ptr += sprintf(ptr, "?");
        }
        ptr += sprintf(ptr, ")");
    }

    char *sql = sqlite3_mprintf(
        "INSERT INTO %s_vectors (%s) VALUES %s",
        vtab->table_name, col_list, values_clause);
    sqlite3_free(col_list);
    sqlite3_free(values_clause);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v3(vtab->db, sql, -1, SQLITE_PREPARE_PERSISTENT, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare batch insert: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // Store in cache
    if (vtab->batch_vectors_stmt)
    {
        vtab->batch_vectors_stmt[rows] = stmt;
    }

    *pStmt = stmt;
    return SQLITE_OK;
}

// Prepare (and cache) a multi-row INSERT for a per-column cluster_vector_map table
static int prepare_batch_map_stmt(vector_index_vtab *vtab, int col_idx, int rows, sqlite3_stmt **pStmt, char **pzErr)
{
    if (rows <= 0 || rows > vtab->buffer_capacity)
    {
        *pzErr = sqlite3_mprintf("Invalid batch rows for map: %d", rows);
        return SQLITE_ERROR;
    }

    if (!vtab->batch_map_stmt || !vtab->batch_map_stmt[col_idx])
    {
        *pStmt = NULL;
        return SQLITE_ERROR;
    }

    if (vtab->batch_map_stmt[col_idx][rows] != NULL)
    {
        *pStmt = vtab->batch_map_stmt[col_idx][rows];
        return SQLITE_OK;
    }

    int values_size = rows * (3 * 3 + 4);
    char *values_clause = (char *)sqlite3_malloc(values_size);
    if (!values_clause)
    {
        *pzErr = sqlite3_mprintf("Out of memory building map VALUES clause");
        return SQLITE_NOMEM;
    }

    char *ptr = values_clause;
    for (int i = 0; i < rows; i++)
    {
        if (i > 0)
            ptr += sprintf(ptr, ",");
        ptr += sprintf(ptr, "(?, ?, ?)");
    }

    char *sql = sqlite3_mprintf(
        "INSERT INTO %s_%s_cluster_vector_map (vector_id, cluster_id, distance) VALUES %s",
        vtab->table_name, vtab->columns[col_idx].name, values_clause);
    sqlite3_free(values_clause);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v3(vtab->db, sql, -1, SQLITE_PREPARE_PERSISTENT, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare batch map insert: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    vtab->batch_map_stmt[col_idx][rows] = stmt;
    *pStmt = stmt;
    return SQLITE_OK;
}

// Prepare cached statements for a vtab (helper)
static int prepare_vtab_statements(vector_index_vtab *vtab, char **pzErr)
{
    char *sql = NULL;
    int rc;

    // insert_vector_stmt - build dynamically based on columns
    // Format: INSERT INTO table_vectors (col1, col2, ...) VALUES (?1, ?2, ..., ?N)
    char *col_list = sqlite3_mprintf("");
    char *param_list = sqlite3_mprintf("");

    for (int i = 0; i < vtab->num_columns; i++)
    {
        char *new_col_list = sqlite3_mprintf("%s%s%s",
                                             col_list,
                                             (i > 0 ? ", " : ""),
                                             vtab->columns[i].name);
        char *new_param_list = sqlite3_mprintf("%s%s?%d",
                                               param_list,
                                               (i > 0 ? ", " : ""),
                                               i + 1);

        sqlite3_free(col_list);
        sqlite3_free(param_list);
        col_list = new_col_list;
        param_list = new_param_list;

        if (!col_list || !param_list)
        {
            sqlite3_free(col_list);
            sqlite3_free(param_list);
            *pzErr = sqlite3_mprintf("Out of memory building insert statement");
            return SQLITE_NOMEM;
        }
    }

    sql = sqlite3_mprintf(
        "INSERT INTO %s_vectors (%s) VALUES (%s)",
        vtab->table_name, col_list, param_list);

    sqlite3_free(col_list);
    sqlite3_free(param_list);

    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &vtab->insert_vector_stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare insert_vector_stmt: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // upsert_map_stmt - DISABLED for multi-column support
    // Each column has its own {table}_{column}_cluster_vector_map table
    // We use dynamic SQL in flush_insert_buffer instead
    vtab->upsert_map_stmt = NULL;

    // delete_map_stmt - DISABLED for multi-column support
    // Each column has its own {table}_{column}_cluster_vector_map table
    // We use dynamic SQL in xDelete instead
    vtab->delete_map_stmt = NULL;

    // inc_cluster_size_stmt - DISABLED for multi-column support
    // Each column has its own {table}_{column}_cluster_tree table
    // We increment cluster size dynamically per column
    vtab->inc_cluster_size_stmt = NULL;

    return SQLITE_OK;
}

// Create shadow tables for the vector index (Hierarchical IVF v2 schema)
static int create_shadow_tables(sqlite3 *db, const char *table_name, ColumnDef *columns, int num_columns, char **pzErr)
{
    char *sql = NULL;
    char *err_msg = NULL;
    int rc;

    // Create _vectors table: Insert-only vector storage with user-defined columns
    // Build column list dynamically: id INTEGER PRIMARY KEY, <user_columns>, created_at INTEGER
    char *column_defs = sqlite3_mprintf("id INTEGER PRIMARY KEY");

    for (int i = 0; i < num_columns; i++)
    {
        char *new_defs = sqlite3_mprintf("%s, %s %s", column_defs, columns[i].name, columns[i].type);
        sqlite3_free(column_defs);
        column_defs = new_defs;
        if (!column_defs)
        {
            *pzErr = sqlite3_mprintf("Out of memory building _vectors table schema");
            return SQLITE_NOMEM;
        }
    }

    if (!column_defs)
    {
        *pzErr = sqlite3_mprintf("Out of memory building _vectors table schema");
        return SQLITE_NOMEM;
    }

    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_vectors (%s)",
        table_name, column_defs);
    sqlite3_free(column_defs);

    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create vectors table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create per-column cluster tables for each vector column
    for (int i = 0; i < num_columns; i++)
    {
        if (!columns[i].is_vector)
        {
            continue;
        }

        const char *col_name = columns[i].name;
        int dimensions = columns[i].dimensions;

        // Create {table}_{column}_cluster_tree table
        sql = sqlite3_mprintf(
            "CREATE TABLE IF NOT EXISTS %s_%s_cluster_tree ("
            "cluster_id INTEGER PRIMARY KEY,"
            "parent_id INTEGER DEFAULT NULL,"
            "centroid_blob BLOB NOT NULL,"
            "is_leaf INTEGER NOT NULL DEFAULT 1,"
            "cluster_size INTEGER DEFAULT 0,"
            "radius REAL DEFAULT 0.0"
            ")",
            table_name, col_name);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);

        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create %s_%s_cluster_tree table: %s", table_name, col_name, err_msg);
            sqlite3_free(err_msg);
            return rc;
        }

        // Create index on parent_id for tree traversal
        sql = sqlite3_mprintf(
            "CREATE INDEX IF NOT EXISTS %s_%s_cluster_tree_parent_idx ON %s_%s_cluster_tree(parent_id)",
            table_name, col_name, table_name, col_name);

        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);

        sqlite3_free(sql);

        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create %s_%s_cluster_tree parent index: %s", table_name, col_name, err_msg);
            sqlite3_free(err_msg);
            return rc;
        }

        // Create index on is_leaf for finding leaf clusters
        sql = sqlite3_mprintf(
            "CREATE INDEX IF NOT EXISTS %s_%s_cluster_tree_leaf_idx ON %s_%s_cluster_tree(is_leaf) WHERE is_leaf = 1",
            table_name, col_name, table_name, col_name);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);
        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create %s_%s_cluster_tree leaf index: %s", table_name, col_name, err_msg);
            sqlite3_free(err_msg);
            return rc;
        }

        // Create {table}_{column}_cluster_vector_map table
        sql = sqlite3_mprintf(
            "CREATE TABLE IF NOT EXISTS %s_%s_cluster_vector_map ("
            "vector_id INTEGER NOT NULL,"
            "cluster_id INTEGER NOT NULL,"
            "distance REAL,"
            "PRIMARY KEY (vector_id)"
            ")",
            table_name, col_name);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);

        sqlite3_free(sql);

        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create %s_%s_cluster_vector_map table: %s", table_name, col_name, err_msg);
            sqlite3_free(err_msg);
            return rc;
        }

        // Create B-tree index on cluster_id for fast retrieval of cluster members
        sql = sqlite3_mprintf(
            "CREATE INDEX IF NOT EXISTS %s_%s_cluster_vector_map_cluster_idx ON %s_%s_cluster_vector_map(cluster_id)",
            table_name, col_name, table_name, col_name);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);
        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create %s_%s_cluster_vector_map cluster index: %s", table_name, col_name, err_msg);
            sqlite3_free(err_msg);
            return rc;
        }

        // Initialize root cluster (cluster_id = 1) with zero centroid
        // Create zero vector of appropriate dimensions
        int blob_size = 2 + 4 + (dimensions * 4); // version + type + dims + data
        unsigned char *zero_blob = (unsigned char *)sqlite3_malloc(blob_size);
        if (!zero_blob)
        {
            *pzErr = sqlite3_mprintf("Out of memory creating zero centroid");
            return SQLITE_NOMEM;
        }

        zero_blob[0] = 0x01; // version
        zero_blob[1] = 0x01; // type: float32
        // dimensions (little-endian uint32)
        zero_blob[2] = (unsigned char)(dimensions & 0xFF);
        zero_blob[3] = (unsigned char)((dimensions >> 8) & 0xFF);
        zero_blob[4] = (unsigned char)((dimensions >> 16) & 0xFF);
        zero_blob[5] = (unsigned char)((dimensions >> 24) & 0xFF);
        // Zero the vector data
        memset(&zero_blob[6], 0, dimensions * 4);

        sql = sqlite3_mprintf(
            "INSERT OR IGNORE INTO %s_%s_cluster_tree (cluster_id, parent_id, centroid_blob, is_leaf, cluster_size) "
            "VALUES (1, NULL, ?, 1, 0)",
            table_name, col_name);

        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc == SQLITE_OK)
        {
            sqlite3_bind_blob(stmt, 1, zero_blob, blob_size, SQLITE_TRANSIENT);
            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
        sqlite3_free(zero_blob);

        if (rc != SQLITE_DONE)
        {
            *pzErr = sqlite3_mprintf("Failed to initialize root cluster for %s: %s", col_name, sqlite3_errmsg(db));
            return rc;
        }
    }

    // Create _metadata table for configuration and state
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_metadata ("
        "key TEXT PRIMARY KEY,"
        "value TEXT NOT NULL"
        ")",
        table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create metadata table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    return SQLITE_OK;
}

// Helper function to parse SQLite type affinity from type string
static int get_type_affinity(const char *type)
{
    if (type == NULL)
        return SQLITE_TEXT;

    // Convert to uppercase for comparison
    char upper_type[64];
    size_t len = strlen(type);
    if (len >= sizeof(upper_type))
        len = sizeof(upper_type) - 1;

    for (size_t i = 0; i < len; i++)
    {
        upper_type[i] = (char)toupper((unsigned char)type[i]);
    }
    upper_type[len] = '\0';

    // SQLite type affinity rules
    if (strstr(upper_type, "INT"))
        return SQLITE_INTEGER;
    if (strstr(upper_type, "CHAR") || strstr(upper_type, "CLOB") || strstr(upper_type, "TEXT"))
        return SQLITE_TEXT;
    if (strstr(upper_type, "BLOB"))
        return SQLITE_BLOB;
    if (strstr(upper_type, "REAL") || strstr(upper_type, "FLOA") || strstr(upper_type, "DOUB"))
        return SQLITE_FLOAT;

    return SQLITE_TEXT; // Default
}

// Parse CREATE VIRTUAL TABLE parameters including column definitions
// Supports: CREATE VIRTUAL TABLE t USING vector_index(col1 TYPE1, col2 BLOB, ..., dimensions=N, col2_dimensions=M, ...)
static int parse_index_params(
    int argc,
    const char *const *argv,
    ColumnDef **columns_out,
    int *num_columns_out,
    int *distance_metric,
    int *max_cluster_size,
    int *min_cluster_size,
    char **pzErr)
{
    // Default values
    *distance_metric = DISTANCE_METRIC_COSINE;
    *max_cluster_size = 5000;
    *min_cluster_size = 200;
    *columns_out = NULL;
    *num_columns_out = 0;

    // First pass: count columns and allocate space
    int max_cols = argc; // Over-allocate
    ColumnDef *columns = sqlite3_malloc(sizeof(ColumnDef) * max_cols);
    if (!columns)
    {
        *pzErr = sqlite3_mprintf("Out of memory allocating column definitions");
        return SQLITE_NOMEM;
    }
    memset(columns, 0, sizeof(ColumnDef) * max_cols);

    int col_count = 0;
    int default_dimensions = 0;

    // Parse arguments: argv[0] = module name, argv[1] = database, argv[2] = table name
    // Parameters start at argv[3]
    for (int i = 3; i < argc; i++)
    {
        const char *arg = argv[i];

        // Check for reserved column names
        if (strcmp(arg, "id") == 0 || strncmp(arg, "id ", 3) == 0)
        {
            *pzErr = sqlite3_mprintf("Cannot define 'id' column - it is automatically generated as the rowid");
            sqlite3_free(columns);
            return SQLITE_ERROR;
        }

        if (strcmp(arg, "rowid") == 0 || strncmp(arg, "rowid ", 6) == 0)
        {
            *pzErr = sqlite3_mprintf("Cannot define 'rowid' column - it is reserved");
            sqlite3_free(columns);
            return SQLITE_ERROR;
        }

        // Check for key=value parameters (not column definitions)
        const char *eq = strchr(arg, '=');
        if (eq != NULL)
        {
            size_t key_len = eq - arg;
            const char *value = eq + 1;

            if (strncmp(arg, "dimensions", key_len) == 0 && key_len == 10)
            {
                // Global dimensions parameter (backward compatibility)
                default_dimensions = atoi(value);
            }
            else if (strncmp(arg, "distance_metric", key_len) == 0)
            {
                if (strcmp(value, "'l2'") == 0 || strcmp(value, "l2") == 0)
                    *distance_metric = DISTANCE_METRIC_L2;
                else if (strcmp(value, "'cosine'") == 0 || strcmp(value, "cosine") == 0)
                    *distance_metric = DISTANCE_METRIC_COSINE;
                else if (strcmp(value, "'dot'") == 0 || strcmp(value, "dot") == 0)
                    *distance_metric = DISTANCE_METRIC_DOT;
                else if (strcmp(value, "'hamming'") == 0 || strcmp(value, "hamming") == 0)
                    *distance_metric = DISTANCE_METRIC_HAMMING;
            }
            else if (strncmp(arg, "max_cluster_size", key_len) == 0)
            {
                *max_cluster_size = atoi(value);
            }
            else if (strncmp(arg, "min_cluster_size", key_len) == 0)
            {
                *min_cluster_size = atoi(value);
            }
            else
            {
                // Check if this is a column-specific dimensions parameter: {column_name}_dimensions=N
                if (key_len > 11 && strncmp(arg + key_len - 11, "_dimensions", 11) == 0)
                {
                    // Extract column name (everything before "_dimensions")
                    char col_name[256];
                    size_t col_name_len = key_len - 11;
                    if (col_name_len >= sizeof(col_name))
                        col_name_len = sizeof(col_name) - 1;
                    strncpy(col_name, arg, col_name_len);
                    col_name[col_name_len] = '\0';

                    // Find the column and set its dimensions
                    int found = 0;
                    for (int j = 0; j < col_count; j++)
                    {
                        if (strcmp(columns[j].name, col_name) == 0)
                        {
                            columns[j].dimensions = atoi(value);
                            found = 1;
                            break;
                        }
                    }

                    if (!found)
                    {
                        *pzErr = sqlite3_mprintf("Column '%s' not found for dimensions parameter", col_name);
                        sqlite3_free(columns);
                        return SQLITE_ERROR;
                    }
                }
                // Check if this is a column-specific distance_metric parameter: {column_name}_distance_metric='metric'
                else if (key_len > 16 && strncmp(arg + key_len - 16, "_distance_metric", 16) == 0)
                {
                    // Extract column name (everything before "_distance_metric")
                    char col_name[256];
                    size_t col_name_len = key_len - 16;

                    if (col_name_len >= sizeof(col_name))
                    {
                        col_name_len = sizeof(col_name) - 1;
                    }

                    strncpy(col_name, arg, col_name_len);
                    col_name[col_name_len] = '\0';

                    // Parse metric value
                    int metric = -1;
                    if (strcmp(value, "'l2'") == 0 || strcmp(value, "l2") == 0)
                        metric = DISTANCE_METRIC_L2;
                    else if (strcmp(value, "'cosine'") == 0 || strcmp(value, "cosine") == 0)
                        metric = DISTANCE_METRIC_COSINE;
                    else if (strcmp(value, "'dot'") == 0 || strcmp(value, "dot") == 0)
                        metric = DISTANCE_METRIC_DOT;
                    else if (strcmp(value, "'hamming'") == 0 || strcmp(value, "hamming") == 0)
                        metric = DISTANCE_METRIC_HAMMING;
                    else
                    {
                        *pzErr = sqlite3_mprintf("Invalid distance_metric value for column '%s': %s", col_name, value);
                        sqlite3_free(columns);
                        return SQLITE_ERROR;
                    }

                    // Find the column and set its distance metric
                    int found = 0;
                    for (int j = 0; j < col_count; j++)
                    {
                        if (strcmp(columns[j].name, col_name) == 0)
                        {
                            columns[j].distance_metric = metric;
                            found = 1;
                            break;
                        }
                    }

                    if (!found)
                    {
                        *pzErr = sqlite3_mprintf("Column '%s' not found for distance_metric parameter", col_name);
                        sqlite3_free(columns);
                        return SQLITE_ERROR;
                    }
                }
            }
            continue;
        }

        // This should be a column definition: "column_name TYPE"
        // Split by space to get name and type
        const char *space = strchr(arg, ' ');
        char *col_name = NULL;
        char *col_type = NULL;

        if (space)
        {
            // Has type: "name TYPE"
            size_t name_len = space - arg;
            col_name = sqlite3_mprintf("%.*s", (int)name_len, arg);
            col_type = sqlite3_mprintf("%s", space + 1);
        }
        else
        {
            // No type specified, default to TEXT (but BLOB columns need explicit type)
            col_name = sqlite3_mprintf("%s", arg);
            col_type = sqlite3_mprintf("TEXT");
        }

        if (!col_name || !col_type)
        {
            sqlite3_free(col_name);
            sqlite3_free(col_type);
            sqlite3_free(columns);
            *pzErr = sqlite3_mprintf("Out of memory parsing column definition");
            return SQLITE_NOMEM;
        }

        // Store column definition
        columns[col_count].name = col_name;
        columns[col_count].type = col_type;
        columns[col_count].affinity = get_type_affinity(col_type);
        columns[col_count].is_vector = (columns[col_count].affinity == SQLITE_BLOB);
        columns[col_count].dimensions = 0;       // Will be set later if specified
        columns[col_count].distance_metric = -1; // Will be set later if specified

        col_count++;
    }

    // Second pass: apply default dimensions and distance_metric to vector columns that don't have specific values
    for (int i = 0; i < col_count; i++)
    {
        if (columns[i].is_vector)
        {
            if (columns[i].dimensions == 0)
            {
                columns[i].dimensions = default_dimensions;
            }
            if (columns[i].distance_metric == -1)
            {
                columns[i].distance_metric = *distance_metric;
            }
        }
    }

    // Validate: at least one vector column with dimensions
    int has_vector = 0;
    for (int i = 0; i < col_count; i++)
    {
        if (columns[i].is_vector)
        {
            if (columns[i].dimensions <= 0)
            {
                *pzErr = sqlite3_mprintf("Vector column '%s' must have dimensions specified", columns[i].name);
                for (int j = 0; j < col_count; j++)
                {
                    sqlite3_free(columns[j].name);
                    sqlite3_free(columns[j].type);
                }
                sqlite3_free(columns);
                return SQLITE_ERROR;
            }
            has_vector = 1;
        }
    }

    if (!has_vector)
    {
        *pzErr = sqlite3_mprintf("At least one BLOB column (vector) is required");
        for (int j = 0; j < col_count; j++)
        {
            sqlite3_free(columns[j].name);
            sqlite3_free(columns[j].type);
        }
        sqlite3_free(columns);
        return SQLITE_ERROR;
    }

    if (*max_cluster_size < *min_cluster_size)
    {
        *pzErr = sqlite3_mprintf("max_cluster_size must be greater than min_cluster_size");
        for (int j = 0; j < col_count; j++)
        {
            sqlite3_free(columns[j].name);
            sqlite3_free(columns[j].type);
        }
        sqlite3_free(columns);
        return SQLITE_ERROR;
    }

    *columns_out = columns;
    *num_columns_out = col_count;
    return SQLITE_OK;
}

// xCreate: Create a new vector index virtual table
int vector_index_create(
    sqlite3 *db,
    void *pAux,
    int argc,
    const char *const *argv,
    sqlite3_vtab **ppVtab,
    char **pzErr)
{
    vector_index_vtab *vtab;
    int rc;
    ColumnDef *columns = NULL;
    int num_columns = 0;
    int distance_metric, max_cluster_size, min_cluster_size;

    // Parse parameters
    rc = parse_index_params(argc, argv, &columns, &num_columns, &distance_metric, &max_cluster_size, &min_cluster_size, pzErr);
    if (rc != SQLITE_OK)
    {
        return rc;
    }

    // Create shadow tables with dynamic column definitions
    rc = create_shadow_tables(db, argv[2], columns, num_columns, pzErr);
    if (rc != SQLITE_OK)
    {
        // Clean up columns
        for (int i = 0; i < num_columns; i++)
        {
            sqlite3_free(columns[i].name);
            sqlite3_free(columns[i].type);
        }
        sqlite3_free(columns);
        return rc;
    }

    // Allocate virtual table structure
    vtab = (vector_index_vtab *)sqlite3_malloc(sizeof(vector_index_vtab));
    if (vtab == NULL)
    {
        // Clean up columns
        for (int i = 0; i < num_columns; i++)
        {
            sqlite3_free(columns[i].name);
            sqlite3_free(columns[i].type);
        }
        sqlite3_free(columns);
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(vector_index_vtab));

    vtab->db = db;
    vtab->pAux = pAux; // Store connection context
    vtab->table_name = sqlite3_mprintf("%s", argv[2]);
    vtab->columns = columns;
    vtab->num_columns = num_columns;

    // Set dimensions from first vector column for backward compatibility
    vtab->dimensions = 0;
    vtab->vector_col_index = -1;
    for (int i = 0; i < num_columns; i++)
    {
        if (columns[i].is_vector)
        {
            if (vtab->dimensions == 0)
            {
                vtab->dimensions = columns[i].dimensions;
                vtab->vector_col_index = i;
            }
            break;
        }
    }

    vtab->distance_metric = distance_metric;
    vtab->max_cluster_size = max_cluster_size;
    vtab->min_cluster_size = min_cluster_size;

    // Declare virtual table schema dynamically based on parsed columns
    // Schema format: CREATE TABLE x(id INTEGER PRIMARY KEY, col1 TYPE1, col2 TYPE2, ...)
    char *schema = sqlite3_mprintf("CREATE TABLE x(id INTEGER PRIMARY KEY");
    for (int i = 0; i < num_columns; i++)
    {
        char *new_schema = sqlite3_mprintf("%s, %s %s", schema, columns[i].name, columns[i].type);
        sqlite3_free(schema);
        schema = new_schema;
        if (!schema)
        {
            // Clean up columns
            for (int j = 0; j < num_columns; j++)
            {
                sqlite3_free(columns[j].name);
                sqlite3_free(columns[j].type);
            }
            sqlite3_free(columns);
            sqlite3_free(vtab->table_name);
            sqlite3_free(vtab);
            return SQLITE_NOMEM;
        }
    }

    char *final_schema = sqlite3_mprintf("%s)", schema);
    sqlite3_free(schema);
    schema = final_schema;

    if (!schema)
    {
        // Clean up columns
        for (int j = 0; j < num_columns; j++)
        {
            sqlite3_free(columns[j].name);
            sqlite3_free(columns[j].type);
        }
        sqlite3_free(columns);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }

    rc = sqlite3_declare_vtab(db, schema);
    sqlite3_free(schema);
    if (rc != SQLITE_OK)
    {
        // Clean up columns
        for (int j = 0; j < num_columns; j++)
        {
            sqlite3_free(columns[j].name);
            sqlite3_free(columns[j].type);
        }
        sqlite3_free(columns);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Store metadata - persist all column definitions so they can be restored on xConnect
    // Store column count first
    char *sql = sqlite3_mprintf(
        "INSERT OR REPLACE INTO %s_metadata (key, value) VALUES "
        "('column_count', '%d'), "
        "('max_cluster_size', '%d'), "
        "('min_cluster_size', '%d')",
        vtab->table_name,
        num_columns,
        max_cluster_size,
        min_cluster_size);

    char *err_msg = NULL;
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to store metadata: %s", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Store each column's metadata
    for (int i = 0; i < num_columns; i++)
    {
        sql = sqlite3_mprintf(
            "INSERT OR REPLACE INTO %s_metadata (key, value) VALUES "
            "('column_%d_name', '%q'), "
            "('column_%d_type', '%q'), "
            "('column_%d_dimensions', '%d'), "
            "('column_%d_distance_metric', '%d')",
            vtab->table_name,
            i, columns[i].name,
            i, columns[i].type,
            i, columns[i].dimensions,
            i, columns[i].distance_metric);

        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);
        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to store column %d metadata: %s", i, err_msg);
            sqlite3_free(err_msg);
            sqlite3_free(vtab->table_name);
            sqlite3_free(vtab);
            return rc;
        }
    }

    // Create root cluster (cluster 0) for each vector column for fast initial assignments
    // All vectors start in cluster 0, then background job reassigns to proper clusters
    for (int i = 0; i < num_columns; i++)
    {
        // Only create root cluster for BLOB columns (vector columns)
        if (strcmp(columns[i].type, "BLOB") != 0)
        {
            continue;
        }

        sql = sqlite3_mprintf(
            "INSERT OR IGNORE INTO %s_%s_cluster_tree (cluster_id, parent_id, centroid_blob, is_leaf, cluster_size) "
            "SELECT 0, NULL, X'00000000', 1, 0 "
            "WHERE NOT EXISTS (SELECT 1 FROM %s_%s_cluster_tree WHERE cluster_id = 0)",
            vtab->table_name, columns[i].name, vtab->table_name, columns[i].name);
        rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
        sqlite3_free(sql);

        if (rc != SQLITE_OK)
        {
            *pzErr = sqlite3_mprintf("Failed to create root cluster for column %s: %s", columns[i].name, err_msg);
            sqlite3_free(err_msg);
            sqlite3_free(vtab->table_name);
            sqlite3_free(vtab);
            return rc;
        }
    }

    // Prepare cached statements for hot paths
    rc = prepare_vtab_statements(vtab, pzErr);
    if (rc != SQLITE_OK)
    {
        // Clean up prepared partial state if any
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Initialize insert buffer
    vtab->buffer_capacity = INSERT_BUFFER_CAPACITY;
    vtab->insert_buffer = (PendingInsert *)sqlite3_malloc(sizeof(PendingInsert) * vtab->buffer_capacity);
    if (vtab->insert_buffer == NULL)
    {
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    // Allocate cached prepared-statement arrays (indexed by row count)
    vtab->batch_vectors_stmt = (sqlite3_stmt **)sqlite3_malloc(sizeof(sqlite3_stmt *) * (vtab->buffer_capacity + 1));
    if (!vtab->batch_vectors_stmt)
    {
        sqlite3_free(vtab->insert_buffer);
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    for (int i = 0; i <= vtab->buffer_capacity; i++)
        vtab->batch_vectors_stmt[i] = NULL;

    vtab->batch_map_stmt = (sqlite3_stmt ***)sqlite3_malloc(sizeof(sqlite3_stmt **) * vtab->num_columns);
    if (!vtab->batch_map_stmt)
    {
        sqlite3_free(vtab->batch_vectors_stmt);
        sqlite3_free(vtab->insert_buffer);
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    for (int c = 0; c < vtab->num_columns; c++)
    {
        if (!vtab->columns[c].is_vector)
        {
            vtab->batch_map_stmt[c] = NULL;
            continue;
        }

        vtab->batch_map_stmt[c] = (sqlite3_stmt **)sqlite3_malloc(sizeof(sqlite3_stmt *) * (vtab->buffer_capacity + 1));
        if (!vtab->batch_map_stmt[c])
        {
            for (int k = 0; k < c; k++)
            {
                if (vtab->batch_map_stmt[k])
                    sqlite3_free(vtab->batch_map_stmt[k]);
            }
            sqlite3_free(vtab->batch_map_stmt);
            sqlite3_free(vtab->batch_vectors_stmt);
            sqlite3_free(vtab->insert_buffer);
            if (vtab->insert_vector_stmt)
                sqlite3_finalize(vtab->insert_vector_stmt);
            if (vtab->upsert_map_stmt)
                sqlite3_finalize(vtab->upsert_map_stmt);
            if (vtab->delete_map_stmt)
                sqlite3_finalize(vtab->delete_map_stmt);
            if (vtab->inc_cluster_size_stmt)
                sqlite3_finalize(vtab->inc_cluster_size_stmt);
            sqlite3_free(vtab->table_name);
            sqlite3_free(vtab);
            return SQLITE_NOMEM;
        }
        for (int r = 0; r <= vtab->buffer_capacity; r++)
            vtab->batch_map_stmt[c][r] = NULL;
    }

    vtab->buffer_size = 0;
    vtab->in_transaction = 0;

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
    char **pzErr)
{
    vector_index_vtab *vtab;
    int rc;

    // Allocate virtual table structure
    vtab = (vector_index_vtab *)sqlite3_malloc(sizeof(vector_index_vtab));
    if (vtab == NULL)
    {
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(vector_index_vtab));

    vtab->db = db;
    vtab->table_name = sqlite3_mprintf("%s", argv[2]);

    // Load metadata - first get column count
    char *sql = sqlite3_mprintf(
        "SELECT value FROM %s_metadata WHERE key = 'column_count'",
        vtab->table_name);

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    int num_columns = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW)
    {
        num_columns = atoi((const char *)sqlite3_column_text(stmt, 0));
    }
    sqlite3_finalize(stmt);

    if (num_columns == 0)
    {
        *pzErr = sqlite3_mprintf("No column metadata found for table %s", vtab->table_name);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_ERROR;
    }

    // Allocate columns array
    vtab->columns = (ColumnDef *)sqlite3_malloc(sizeof(ColumnDef) * num_columns);
    if (!vtab->columns)
    {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    memset(vtab->columns, 0, sizeof(ColumnDef) * num_columns);
    vtab->num_columns = num_columns;

    // Load each column's metadata
    for (int i = 0; i < num_columns; i++)
    {
        // Load column name
        sql = sqlite3_mprintf("SELECT value FROM %s_metadata WHERE key = 'column_%d_name'",
                              vtab->table_name, i);
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
        {
            vtab->columns[i].name = sqlite3_mprintf("%s", sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);

        // Load column type
        sql = sqlite3_mprintf("SELECT value FROM %s_metadata WHERE key = 'column_%d_type'",
                              vtab->table_name, i);
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
        {
            vtab->columns[i].type = sqlite3_mprintf("%s", sqlite3_column_text(stmt, 0));
            vtab->columns[i].affinity = get_type_affinity(vtab->columns[i].type);
            vtab->columns[i].is_vector = (vtab->columns[i].affinity == SQLITE_BLOB);
        }
        sqlite3_finalize(stmt);

        // Load column dimensions
        sql = sqlite3_mprintf("SELECT value FROM %s_metadata WHERE key = 'column_%d_dimensions'",
                              vtab->table_name, i);
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
        {
            vtab->columns[i].dimensions = atoi((const char *)sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);

        // Load column distance_metric
        sql = sqlite3_mprintf("SELECT value FROM %s_metadata WHERE key = 'column_%d_distance_metric'",
                              vtab->table_name, i);
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
        {
            vtab->columns[i].distance_metric = atoi((const char *)sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);

        // Set first vector column for backward compatibility
        if (vtab->columns[i].is_vector && vtab->vector_col_index == -1)
        {
            vtab->vector_col_index = i;
            vtab->dimensions = vtab->columns[i].dimensions;
            vtab->distance_metric = vtab->columns[i].distance_metric;
        }
    }

    // Load max_cluster_size and min_cluster_size
    sql = sqlite3_mprintf(
        "SELECT value FROM %s_metadata WHERE key = 'max_cluster_size' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'min_cluster_size'",
        vtab->table_name, vtab->table_name);

    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    int row = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        const char *value = (const char *)sqlite3_column_text(stmt, 0);
        if (row == 0)
            vtab->max_cluster_size = atoi(value);
        else if (row == 1)
            vtab->min_cluster_size = atoi(value);
        row++;
    }
    sqlite3_finalize(stmt);

    // Build dynamic schema for sqlite3_declare_vtab
    char *schema = sqlite3_mprintf("CREATE TABLE x(id INTEGER PRIMARY KEY");
    for (int i = 0; i < num_columns; i++)
    {
        char *new_schema = sqlite3_mprintf("%s, %s %s", schema, vtab->columns[i].name, vtab->columns[i].type);
        sqlite3_free(schema);
        schema = new_schema;
    }
    char *final_schema = sqlite3_mprintf("%s)", schema);
    sqlite3_free(schema);

    rc = sqlite3_declare_vtab(db, final_schema);
    sqlite3_free(final_schema);
    if (rc != SQLITE_OK)
    {
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Prepare cached statements for hot paths
    rc = prepare_vtab_statements(vtab, pzErr);
    if (rc != SQLITE_OK)
    {
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
    }

    // Initialize insert buffer
    vtab->buffer_capacity = INSERT_BUFFER_CAPACITY;
    vtab->insert_buffer = (PendingInsert *)sqlite3_malloc(sizeof(PendingInsert) * vtab->buffer_capacity);
    if (vtab->insert_buffer == NULL)
    {
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    // Allocate cached prepared-statement arrays (indexed by row count)
    vtab->batch_vectors_stmt = (sqlite3_stmt **)sqlite3_malloc(sizeof(sqlite3_stmt *) * (vtab->buffer_capacity + 1));
    if (!vtab->batch_vectors_stmt)
    {
        sqlite3_free(vtab->insert_buffer);
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    for (int i = 0; i <= vtab->buffer_capacity; i++)
        vtab->batch_vectors_stmt[i] = NULL;

    vtab->batch_map_stmt = (sqlite3_stmt ***)sqlite3_malloc(sizeof(sqlite3_stmt **) * vtab->num_columns);
    if (!vtab->batch_map_stmt)
    {
        sqlite3_free(vtab->batch_vectors_stmt);
        sqlite3_free(vtab->insert_buffer);
        if (vtab->insert_vector_stmt)
            sqlite3_finalize(vtab->insert_vector_stmt);
        if (vtab->upsert_map_stmt)
            sqlite3_finalize(vtab->upsert_map_stmt);
        if (vtab->delete_map_stmt)
            sqlite3_finalize(vtab->delete_map_stmt);
        if (vtab->inc_cluster_size_stmt)
            sqlite3_finalize(vtab->inc_cluster_size_stmt);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_NOMEM;
    }
    for (int c = 0; c < vtab->num_columns; c++)
    {
        if (!vtab->columns[c].is_vector)
        {
            vtab->batch_map_stmt[c] = NULL;
            continue;
        }

        vtab->batch_map_stmt[c] = (sqlite3_stmt **)sqlite3_malloc(sizeof(sqlite3_stmt *) * (vtab->buffer_capacity + 1));
        if (!vtab->batch_map_stmt[c])
        {
            for (int k = 0; k < c; k++)
            {
                if (vtab->batch_map_stmt[k])
                    sqlite3_free(vtab->batch_map_stmt[k]);
            }
            sqlite3_free(vtab->batch_map_stmt);
            sqlite3_free(vtab->batch_vectors_stmt);
            sqlite3_free(vtab->insert_buffer);
            if (vtab->insert_vector_stmt)
                sqlite3_finalize(vtab->insert_vector_stmt);
            if (vtab->upsert_map_stmt)
                sqlite3_finalize(vtab->upsert_map_stmt);
            if (vtab->delete_map_stmt)
                sqlite3_finalize(vtab->delete_map_stmt);
            if (vtab->inc_cluster_size_stmt)
                sqlite3_finalize(vtab->inc_cluster_size_stmt);
            sqlite3_free(vtab->table_name);
            sqlite3_free(vtab);
            return SQLITE_NOMEM;
        }
        for (int r = 0; r <= vtab->buffer_capacity; r++)
            vtab->batch_map_stmt[c][r] = NULL;
    }

    vtab->buffer_size = 0;
    vtab->in_transaction = 0;

    *ppVtab = (sqlite3_vtab *)vtab;
    return SQLITE_OK;
}

// xBestIndex: Query planner
int vector_index_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo)
{
    pIdxInfo->estimatedCost = 1000.0;
    pIdxInfo->estimatedRows = 1000;
    return SQLITE_OK;
}

// xDisconnect: Disconnect from virtual table
int vector_index_disconnect(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;

    // Flush any remaining buffered inserts before disconnect
    if (vtab->buffer_size > 0)
    {
        char *err_msg = NULL;
        flush_insert_buffer(vtab, &err_msg);
        // Ignore errors on disconnect - data will be lost but at least we tried
        if (err_msg)
        {
            sqlite3_free(err_msg);
        }
    }

    // Free any buffered inserts
    if (vtab->insert_buffer)
    {
        for (int i = 0; i < vtab->buffer_size; i++)
        {
            if (vtab->insert_buffer[i].column_values)
            {
                for (int j = 0; j < vtab->insert_buffer[i].num_columns; j++)
                {
                    sqlite3_value_free(vtab->insert_buffer[i].column_values[j]);
                }
                sqlite3_free(vtab->insert_buffer[i].column_values);
            }
        }
        sqlite3_free(vtab->insert_buffer);
        vtab->insert_buffer = NULL;
    }

    // Finalize cached statements
    if (vtab->insert_vector_stmt)
    {
        sqlite3_finalize(vtab->insert_vector_stmt);
        vtab->insert_vector_stmt = NULL;
    }
    if (vtab->upsert_map_stmt)
    {
        sqlite3_finalize(vtab->upsert_map_stmt);
        vtab->upsert_map_stmt = NULL;
    }
    if (vtab->delete_map_stmt)
    {
        sqlite3_finalize(vtab->delete_map_stmt);
        vtab->delete_map_stmt = NULL;
    }
    if (vtab->inc_cluster_size_stmt)
    {
        sqlite3_finalize(vtab->inc_cluster_size_stmt);
        vtab->inc_cluster_size_stmt = NULL;
    }

    // Finalize and free cached batch prepared statements
    if (vtab->batch_vectors_stmt)
    {
        for (int i = 0; i <= vtab->buffer_capacity; i++)
        {
            if (vtab->batch_vectors_stmt[i])
                sqlite3_finalize(vtab->batch_vectors_stmt[i]);
        }
        sqlite3_free(vtab->batch_vectors_stmt);
        vtab->batch_vectors_stmt = NULL;
    }

    if (vtab->batch_map_stmt)
    {
        for (int c = 0; c < vtab->num_columns; c++)
        {
            if (vtab->batch_map_stmt[c])
            {
                for (int r = 0; r <= vtab->buffer_capacity; r++)
                {
                    if (vtab->batch_map_stmt[c][r])
                        sqlite3_finalize(vtab->batch_map_stmt[c][r]);
                }
                sqlite3_free(vtab->batch_map_stmt[c]);
            }
        }
        sqlite3_free(vtab->batch_map_stmt);
        vtab->batch_map_stmt = NULL;
    }

    // Free column definitions
    if (vtab->columns)
    {
        for (int i = 0; i < vtab->num_columns; i++)
        {
            sqlite3_free(vtab->columns[i].name);
            sqlite3_free(vtab->columns[i].type);
        }
        sqlite3_free(vtab->columns);
        vtab->columns = NULL;
    }

    // Finalize all cached Go-side prepared statements (cluster tree SELECT,
    // cluster size/centroid UPDATE) before the connection is closed.
    goFinalizeClusterStmts(vtab->db);

    sqlite3_free(vtab->table_name);
    sqlite3_free(vtab);
    return SQLITE_OK;
}

// xDestroy: Drop the virtual table and shadow tables
int vector_index_destroy(sqlite3_vtab *pVtab)
{
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

// Flush buffered inserts to shadow tables using batched SQL
static int flush_insert_buffer(vector_index_vtab *vtab, char **pzErr)
{
    if (vtab->buffer_size == 0)
    {
        return SQLITE_OK;
    }

    int rc;
    char *sql = NULL;
    char *err_msg = NULL;

    // Build multi-row INSERT for _vectors table with all user-defined columns
    // Build column list from vtab->columns
    char *col_list = sqlite3_mprintf("");
    for (int i = 0; i < vtab->num_columns; i++)
    {
        char *new_col_list = sqlite3_mprintf("%s%s%s",
                                             col_list,
                                             (i > 0 ? ", " : ""),
                                             vtab->columns[i].name);
        sqlite3_free(col_list);
        col_list = new_col_list;
        if (!col_list)
        {
            *pzErr = sqlite3_mprintf("Out of memory building column list");
            return SQLITE_NOMEM;
        }
    }

    // Build VALUES clause: (?,?,...,?),(?,?,...,?),...
    // Each row has num_columns parameters
    int params_per_row = vtab->num_columns;
    int values_size = vtab->buffer_size * (params_per_row * 3 + 4); // Conservative: "(?,...,?)," per row
    char *values_clause = (char *)sqlite3_malloc(values_size);
    if (!values_clause)
    {
        sqlite3_free(col_list);
        *pzErr = sqlite3_mprintf("Out of memory building VALUES clause");
        return SQLITE_NOMEM;
    }

    char *ptr = values_clause;
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (i > 0)
        {
            ptr += sprintf(ptr, ",");
        }
        ptr += sprintf(ptr, "(");
        for (int j = 0; j < params_per_row; j++)
        {
            if (j > 0)
            {
                ptr += sprintf(ptr, ",");
            }
            ptr += sprintf(ptr, "?");
        }
        ptr += sprintf(ptr, ")");
    }

    /* Free the locally-built column/values buffers before using cached prepared statements */
    sqlite3_free(col_list);
    sqlite3_free(values_clause);

    sqlite3_stmt *stmt = NULL;
    rc = prepare_batch_vector_stmt(vtab, vtab->buffer_size, &stmt, pzErr);
    if (rc != SQLITE_OK)
    {
        return rc;
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    // Bind all parameters for all rows
    int param_idx = 1;
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        // Note: column_values[0] is the id column (from argv[2]), skip it
        for (int j = 0; j < vtab->num_columns; j++)
        {
            int col_values_idx = j + 1; // +1 to skip id column at index 0

            if (col_values_idx < vtab->insert_buffer[i].num_columns)
            {
                sqlite3_value *val = vtab->insert_buffer[i].column_values[col_values_idx];
                int vtype = sqlite3_value_type(val);

                switch (vtype)
                {
                case SQLITE_INTEGER:
                    sqlite3_bind_int64(stmt, param_idx++, sqlite3_value_int64(val));
                    break;
                case SQLITE_FLOAT:
                    sqlite3_bind_double(stmt, param_idx++, sqlite3_value_double(val));
                    break;
                case SQLITE_TEXT:
                {
                    const char *txt = (const char *)sqlite3_value_text(val);
                    int len = sqlite3_value_bytes(val);
                    sqlite3_bind_text(stmt, param_idx++, txt, len, SQLITE_STATIC);
                    break;
                }
                case SQLITE_BLOB:
                {
                    const void *blob = sqlite3_value_blob(val);
                    int blen = sqlite3_value_bytes(val);
                    sqlite3_bind_blob(stmt, param_idx++, blob, blen, SQLITE_STATIC);
                    break;
                }
                case SQLITE_NULL:
                default:
                    sqlite3_bind_null(stmt, param_idx++);
                    break;
                }
            }
            else
            {
                sqlite3_bind_null(stmt, param_idx++);
            }
        }
    }

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE)
    {
        *pzErr = sqlite3_mprintf("Batch insert failed: %s", sqlite3_errmsg(vtab->db));
        sqlite3_reset(stmt);
        return rc;
    }

    /* Reset cached stmt for reuse (do not finalize) */
    sqlite3_reset(stmt);

    // For a batched multi-row INSERT, sqlite3_last_insert_rowid returns the
    // rowid of the last row inserted.  Rows receive sequential IDs, so the
    // first rowid of this batch is simply last - buffer_size + 1.
    // Using arithmetic here avoids a per-flush SELECT MAX(id) prepare+step.
    sqlite3_int64 last_rowid = sqlite3_last_insert_rowid(vtab->db);
    sqlite3_int64 first_rowid = last_rowid - vtab->buffer_size + 1;

    // Assign vectors to their correct leaf clusters inline using the same db
    // connection that owns this transaction.  goAssignVectorsInBatch reads the
    // cluster tree on vtab->db, so there is no WAL lock contention.
    // cluster_id=0 is never written — no background job is required.
    for (int col_idx = 0; col_idx < vtab->num_columns; col_idx++)
    {
        if (!vtab->columns[col_idx].is_vector)
            continue;

        const char *col_name = vtab->columns[col_idx].name;
        int col_dist_metric = vtab->columns[col_idx].distance_metric;
        int val_idx = col_idx + 1; // +1 to skip id slot in column_values

        // Build blob pointer/length arrays from the buffered insert values.
        void **blob_ptrs = (void **)sqlite3_malloc(sizeof(void *) * vtab->buffer_size);
        int *blob_lens = (int *)sqlite3_malloc(sizeof(int) * vtab->buffer_size);
        sqlite3_int64 *cluster_ids = (sqlite3_int64 *)sqlite3_malloc(sizeof(sqlite3_int64) * vtab->buffer_size);
        double *distances = (double *)sqlite3_malloc(sizeof(double) * vtab->buffer_size);

        if (!blob_ptrs || !blob_lens || !cluster_ids || !distances)
        {
            sqlite3_free(blob_ptrs);
            sqlite3_free(blob_lens);
            sqlite3_free(cluster_ids);
            sqlite3_free(distances);
            *pzErr = sqlite3_mprintf("Out of memory for inline cluster assignment");
            return SQLITE_NOMEM;
        }

        for (int i = 0; i < vtab->buffer_size; i++)
        {
            cluster_ids[i] = 1; // safe default: root cluster
            distances[i] = 0.0;

            if (val_idx < vtab->insert_buffer[i].num_columns &&
                vtab->insert_buffer[i].column_values[val_idx] != NULL)
            {
                blob_ptrs[i] = (void *)sqlite3_value_blob(vtab->insert_buffer[i].column_values[val_idx]);
                blob_lens[i] = sqlite3_value_bytes(vtab->insert_buffer[i].column_values[val_idx]);
            }
            else
            {
                blob_ptrs[i] = NULL;
                blob_lens[i] = 0;
            }
        }

        goAssignVectorsInBatch(
            vtab->db,
            vtab->table_name,
            (char *)col_name,
            col_dist_metric,
            vtab->buffer_size,
            blob_ptrs,
            blob_lens,
            cluster_ids,
            distances);

        // Use cached prepared statement for map insert per column and bind triplets
        sqlite3_stmt *map_stmt = NULL;
        rc = prepare_batch_map_stmt(vtab, col_idx, vtab->buffer_size, &map_stmt, pzErr);
        if (rc != SQLITE_OK)
        {
            sqlite3_free(blob_ptrs);
            sqlite3_free(blob_lens);
            sqlite3_free(cluster_ids);
            sqlite3_free(distances);
            return rc;
        }

        sqlite3_reset(map_stmt);
        sqlite3_clear_bindings(map_stmt);

        int map_param_idx = 1;
        for (int i = 0; i < vtab->buffer_size; i++)
        {
            sqlite3_bind_int64(map_stmt, map_param_idx++, first_rowid + i);
            sqlite3_bind_int64(map_stmt, map_param_idx++, cluster_ids[i]);
            sqlite3_bind_double(map_stmt, map_param_idx++, distances[i]);
        }

        rc = sqlite3_step(map_stmt);
        if (rc != SQLITE_DONE)
        {
            sqlite3_reset(map_stmt);
            sqlite3_free(blob_ptrs);
            sqlite3_free(blob_lens);
            sqlite3_free(cluster_ids);
            sqlite3_free(distances);
            *pzErr = sqlite3_mprintf("Batch map insert failed for column %s: %s", col_name, sqlite3_errmsg(vtab->db));
            return rc;
        }

        sqlite3_reset(map_stmt);

        // ---- Cluster stats (size + centroid) update ----
        // Pass per-row cluster IDs and blob data to Go; it will aggregate
        // per-cluster vector sums and update cluster_size + centroid_blob.
        // blob_ptrs and blob_lens are still valid (not freed yet).
        goUpdateClusterStats(
            vtab->db,
            vtab->table_name,
            (char *)col_name,
            vtab->columns[col_idx].dimensions,
            vtab->buffer_size,
            cluster_ids,
            blob_lens,
            blob_ptrs,
            blob_lens);

        sqlite3_free(blob_ptrs);
        sqlite3_free(blob_lens);
        sqlite3_free(cluster_ids);
        sqlite3_free(distances);
    }

    // NOTE: goTriggerClusterSplits is intentionally NOT called here.
    // flush_insert_buffer runs during xSync (before SQLite COMMIT), so the write
    // lock is still held.  Firing a split goroutine here causes it to block in
    // CompactionPassiveBarrier waiting for that lock.  Instead, the split is
    // triggered from xCommit, after the transaction has fully committed.

    // Free buffered column values
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (vtab->insert_buffer[i].column_values)
        {
            for (int j = 0; j < vtab->insert_buffer[i].num_columns; j++)
            {
                sqlite3_value_free(vtab->insert_buffer[i].column_values[j]);
            }
            sqlite3_free(vtab->insert_buffer[i].column_values);
            vtab->insert_buffer[i].column_values = NULL;
        }
    }

    vtab->buffer_size = 0;
    return SQLITE_OK;
}

// xBegin: Start a transaction
int vector_index_begin(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    vtab->in_transaction = 1;

    // Enlarge the per-connection page cache for the duration of the insert
    // transaction.  PRAGMA cache_size is connection-local and has no effect on
    // WAL mode, other connections, or checkpoint behaviour.  Keeping hot B-tree
    // internal pages in memory across flush_insert_buffer batches reduces VFS
    // round-trips per sqlite3_step.  Negative value = kibibytes (SQLite 3.7.10+).
    // Restored to 0 in xCommit / xRollback.
    sqlite3_exec(vtab->db, "PRAGMA cache_size = -65536", NULL, NULL, NULL);

    return SQLITE_OK;
}

// xSync: Prepare to commit (flush buffer here)
int vector_index_sync(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    char *err_msg = NULL;
    int rc = flush_insert_buffer(vtab, &err_msg);
    if (rc != SQLITE_OK)
    {
        pVtab->zErrMsg = err_msg;
        return rc;
    }
    return SQLITE_OK;
}

// xCommit: Commit transaction
int vector_index_commit(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;

    // Defensive flush in case xSync wasn't called
    if (vtab->buffer_size > 0)
    {
        char *err_msg = NULL;
        int rc = flush_insert_buffer(vtab, &err_msg);
        if (rc != SQLITE_OK)
        {
            pVtab->zErrMsg = err_msg;
            return rc;
        }
    }

    // Restore page cache to 0 (disabled) so subsequent reads go through the
    // distributed VFS layer as required by the normal connection configuration.
    sqlite3_exec(vtab->db, "PRAGMA cache_size = 0", NULL, NULL, NULL);

    vtab->in_transaction = 0;

    // Trigger cluster splits AFTER the SQLite transaction commits so the write
    // lock has been released.  Splits are scheduled as a post-commit hook on
    // the same connection to reuse the warm page cache.
    VectorIndexContext *ctx = (VectorIndexContext *)vtab->pAux;
    if (ctx && ctx->databaseID && ctx->branchID)
    {
        goTriggerClusterSplits(ctx->databaseID, ctx->branchID, vtab->table_name, (uintptr_t)vtab->db);
    }

    return SQLITE_OK;
}

// xRollback: Rollback transaction (clear buffer without flushing)
int vector_index_rollback(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;

    // Restore page cache to 0 on rollback as well.
    sqlite3_exec(vtab->db, "PRAGMA cache_size = 0", NULL, NULL, NULL);

    // Free buffered column values without flushing
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (vtab->insert_buffer[i].column_values)
        {
            for (int j = 0; j < vtab->insert_buffer[i].num_columns; j++)
            {
                sqlite3_value_free(vtab->insert_buffer[i].column_values[j]);
            }
            sqlite3_free(vtab->insert_buffer[i].column_values);
            vtab->insert_buffer[i].column_values = NULL;
        }
    }

    vtab->buffer_size = 0;
    vtab->in_transaction = 0;
    return SQLITE_OK;
}

// xOpen: Open a cursor
int vector_index_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    vector_index_cursor *cursor = (vector_index_cursor *)sqlite3_malloc(sizeof(vector_index_cursor));
    if (cursor == NULL)
    {
        return SQLITE_NOMEM;
    }
    memset(cursor, 0, sizeof(vector_index_cursor));
    cursor->eof = 1;
    *ppCursor = (sqlite3_vtab_cursor *)cursor;
    return SQLITE_OK;
}

// xClose: Close a cursor
int vector_index_close(sqlite3_vtab_cursor *pCursor)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    if (cursor->stmt)
    {
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
    sqlite3_value **argv)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    vector_index_vtab *vtab = (vector_index_vtab *)pCursor->pVtab;

    // Flush any buffered inserts before reading to ensure consistency
    if (vtab->buffer_size > 0)
    {
        char *err_msg = NULL;
        int rc = flush_insert_buffer(vtab, &err_msg);
        if (rc != SQLITE_OK)
        {
            pCursor->pVtab->zErrMsg = err_msg;
            return rc;
        }
    }

    // Query vectors via JOIN with cluster_vector_map (Hierarchical IVF v2 schema)
    // Use first vector column name dynamically for the JOIN
    const char *vector_col_name = "vector"; // default
    for (int i = 0; i < vtab->num_columns; i++)
    {
        if (vtab->columns[i].is_vector)
        {
            vector_col_name = vtab->columns[i].name;
            break;
        }
    }

    // Build column list: "v.id, v.col1, v.col2, ..." to select ALL user-defined columns
    char *column_list = sqlite3_mprintf("v.id");
    for (int i = 0; i < vtab->num_columns; i++)
    {
        char *new_list = sqlite3_mprintf("%s, v.%s", column_list, vtab->columns[i].name);
        sqlite3_free(column_list);
        column_list = new_list;
        if (!column_list)
        {
            return SQLITE_NOMEM;
        }
    }

    char *sql = sqlite3_mprintf(
        "SELECT %s FROM %s_vectors v "
        "INNER JOIN %s_%s_cluster_vector_map m ON v.id = m.vector_id",
        column_list, vtab->table_name, vtab->table_name, vector_col_name);
    sqlite3_free(column_list);

    int rc = sqlite3_prepare_v2(vtab->db, sql, -1, &cursor->stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        return rc;
    }

    cursor->eof = 0;
    return vector_index_next(pCursor);
}

// xNext: Advance cursor
int vector_index_next(sqlite3_vtab_cursor *pCursor)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    int rc = sqlite3_step(cursor->stmt);
    if (rc == SQLITE_ROW)
    {
        cursor->eof = 0;
        return SQLITE_OK;
    }
    cursor->eof = 1;
    return SQLITE_OK;
}

// xEof: Check if cursor is at end
int vector_index_eof(sqlite3_vtab_cursor *pCursor)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    return cursor->eof;
}

// xColumn: Return column value
int vector_index_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int i)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    sqlite3_result_value(ctx, sqlite3_column_value(cursor->stmt, i));
    return SQLITE_OK;
}

// xRowid: Return rowid
int vector_index_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
    vector_index_cursor *cursor = (vector_index_cursor *)pCursor;
    *pRowid = sqlite3_column_int64(cursor->stmt, 0);
    return SQLITE_OK;
}

// xUpdate: Handle INSERT/UPDATE/DELETE operations
int vector_index_update(
    sqlite3_vtab *pVtab,
    int argc,
    sqlite3_value **argv,
    sqlite3_int64 *pRowid)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    char *sql = NULL;
    char *err_msg = NULL;
    int rc;

    // Determine operation type
    if (argc == 1)
    {
        // DELETE: argc == 1, argv[0] = rowid (vector_id)
        sqlite3_int64 vector_id = sqlite3_value_int64(argv[0]);
        sqlite3_stmt *stmt;

        // Multi-column support: Delete from all column-specific cluster_vector_map tables
        // For each vector column, delete the mapping entry
        for (int i = 0; i < vtab->num_columns; i++)
        {
            if (!vtab->columns[i].is_vector)
            {
                continue;
            }

            sql = sqlite3_mprintf(
                "DELETE FROM %s_%s_cluster_vector_map WHERE vector_id = ?",
                vtab->table_name, vtab->columns[i].name);

            rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
            sqlite3_free(sql);

            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("Failed to prepare DELETE for %s_%s_cluster_vector_map: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }

            sqlite3_bind_int64(stmt, 1, vector_id);
            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("DELETE from %s_%s_cluster_vector_map failed: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }
        }

        // Also delete from the _vectors shadow table
        sql = sqlite3_mprintf(
            "DELETE FROM %s_vectors WHERE id = ?",
            vtab->table_name);

        rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);

        if (rc != SQLITE_OK)
        {
            pVtab->zErrMsg = sqlite3_mprintf("Failed to prepare DELETE for %s_vectors: %s",
                                             vtab->table_name, sqlite3_errmsg(vtab->db));
            return rc;
        }

        sqlite3_bind_int64(stmt, 1, vector_id);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE && rc != SQLITE_OK)
        {
            pVtab->zErrMsg = sqlite3_mprintf("DELETE from %s_vectors failed: %s",
                                             vtab->table_name, sqlite3_errmsg(vtab->db));
            return rc;
        }
    }
    else if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL)
    {
        // INSERT: argv[0] = NULL, argv[1] = new rowid, argv[2..argc-1] = column values
        // argc = 2 + num_columns (argv[0] is NULL, argv[1] is rowid, rest are columns)
        int num_cols = argc - 2;

        // Check if buffer is full - flush if needed
        if (vtab->buffer_size >= vtab->buffer_capacity)
        {
            char *flush_err = NULL;
            rc = flush_insert_buffer(vtab, &flush_err);
            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = flush_err;
                return rc;
            }
        }

        // Validate dimensions for BLOB (vector) columns before buffering
        // Note: SQLite passes columns as: argv[0]=NULL, argv[1]=rowid, argv[2]=id, argv[3]=col1, argv[4]=col2, ...
        // The 'id' column (INTEGER PRIMARY KEY) is at argv[2], user columns start at argv[3]
        for (int i = 0; i < vtab->num_columns; i++)
        {
            int col_index = 2 + 1 + i; // Skip argv[0], argv[1], and argv[2] (id column)

            if (col_index >= argc)
            {
                break; // No more columns in argv
            }

            if (vtab->columns[i].is_vector && sqlite3_value_type(argv[col_index]) == SQLITE_BLOB)
            {
                const unsigned char *blob_data = (const unsigned char *)sqlite3_value_blob(argv[col_index]);
                int blob_size = sqlite3_value_bytes(argv[col_index]);

                // Need at least 6 bytes for header (version + type + dimensions)
                if (blob_size < 6)
                {
                    pVtab->zErrMsg = sqlite3_mprintf(
                        "Column '%s' has invalid vector BLOB (too small, expected at least 6 bytes)",
                        vtab->columns[i].name);
                    return SQLITE_ERROR;
                }

                // Extract dimensions from bytes 2-5 (little-endian)
                int actual_dims = (int)blob_data[2] |
                                  ((int)blob_data[3] << 8) |
                                  ((int)blob_data[4] << 16) |
                                  ((int)blob_data[5] << 24);

                // Validate dimensions match column definition
                if (actual_dims != vtab->columns[i].dimensions)
                {
                    pVtab->zErrMsg = sqlite3_mprintf(
                        "Column '%s' expects %d dimensions, got %d",
                        vtab->columns[i].name,
                        vtab->columns[i].dimensions,
                        actual_dims);
                    return SQLITE_ERROR;
                }
            }
        }

        // Allocate array to hold column values
        sqlite3_value **col_vals = (sqlite3_value **)sqlite3_malloc(num_cols * sizeof(sqlite3_value *));
        if (!col_vals)
        {
            pVtab->zErrMsg = sqlite3_mprintf("Out of memory buffering insert");
            return SQLITE_NOMEM;
        }

        // Copy all column values (sqlite3_value_dup makes owned copies)
        for (int i = 0; i < num_cols; i++)
        {
            col_vals[i] = sqlite3_value_dup(argv[2 + i]);
            if (!col_vals[i])
            {
                // Clean up already duplicated values
                for (int j = 0; j < i; j++)
                {
                    sqlite3_value_free(col_vals[j]);
                }
                sqlite3_free(col_vals);
                pVtab->zErrMsg = sqlite3_mprintf("Out of memory duplicating column value");
                return SQLITE_NOMEM;
            }
        }

        vtab->insert_buffer[vtab->buffer_size].column_values = col_vals;
        vtab->insert_buffer[vtab->buffer_size].num_columns = num_cols;
        vtab->buffer_size++;

        // Set pRowid to a temporary value (will be corrected on flush)
        *pRowid = 0;
        sql = NULL;
    }
    else if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL && sqlite3_value_type(argv[1]) != SQLITE_NULL)
    {
        // UPDATE: argv[0] = old rowid, argv[1] = new rowid, argv[2+] = columns
        sqlite3_int64 old_vector_id = sqlite3_value_int64(argv[0]);
        const void *vector_data = sqlite3_value_blob(argv[3]);
        int vector_size = sqlite3_value_bytes(argv[3]);
        sqlite3_stmt *stmt;

        // Multi-column support: Delete old mappings from all column-specific cluster_vector_map tables
        for (int i = 0; i < vtab->num_columns; i++)
        {
            if (!vtab->columns[i].is_vector)
            {
                continue;
            }

            sql = sqlite3_mprintf(
                "DELETE FROM %s_%s_cluster_vector_map WHERE vector_id = ?",
                vtab->table_name, vtab->columns[i].name);

            rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
            sqlite3_free(sql);

            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("Failed to prepare UPDATE DELETE for %s_%s_cluster_vector_map: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }

            sqlite3_bind_int64(stmt, 1, old_vector_id);
            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE delete from %s_%s_cluster_vector_map failed: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }
        }

        // Insert new vector using first vector column name
        const char *vector_col_name = "vector"; // default
        for (int i = 0; i < vtab->num_columns; i++)
        {
            if (vtab->columns[i].is_vector)
            {
                vector_col_name = vtab->columns[i].name;
                break;
            }
        }

        sql = sqlite3_mprintf(
            "INSERT INTO %s_vectors (%s) VALUES (?1)",
            vtab->table_name, vector_col_name);

        rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc != SQLITE_OK)
            return rc;

        sqlite3_bind_blob(stmt, 1, vector_data, vector_size, SQLITE_TRANSIENT);

        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE)
            return rc;

        sqlite3_int64 new_vector_id = sqlite3_last_insert_rowid(vtab->db);

        // Assign new vector to cluster 0 for all vector columns
        for (int i = 0; i < vtab->num_columns; i++)
        {
            if (!vtab->columns[i].is_vector)
            {
                continue;
            }

            sql = sqlite3_mprintf(
                "INSERT INTO %s_%s_cluster_vector_map (vector_id, cluster_id) VALUES (?, ?)",
                vtab->table_name, vtab->columns[i].name);

            rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
            sqlite3_free(sql);

            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("Failed to prepare INSERT for %s_%s_cluster_vector_map: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }

            sqlite3_bind_int64(stmt, 1, new_vector_id);
            sqlite3_bind_int64(stmt, 2, 0);

            rc = sqlite3_step(stmt);
            sqlite3_finalize(stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE assign to %s_%s_cluster_vector_map failed: %s",
                                                 vtab->table_name, vtab->columns[i].name, sqlite3_errmsg(vtab->db));
                return rc;
            }
        }

        *pRowid = new_vector_id;
        sql = NULL;
    }

    return SQLITE_OK;
}

// Register vector_index module (initial registration without context)
int sqlite3_register_vector_index(sqlite3 *db)
{
    return sqlite3_create_module(db, "vector_index", &vector_index_module, NULL);
}

// Register vector_index module with context for per-connection registration
int sqlite3_register_vector_index_with_context(
    sqlite3 *db,
    void *ctxPtr,
    const char *vfsID,
    const char *databaseID,
    const char *branchID)
{
    VectorIndexContext *ctx = (VectorIndexContext *)ctxPtr;
    ctx->vfsID = (char *)vfsID;
    ctx->databaseID = (char *)databaseID;
    ctx->branchID = (char *)branchID;
    return sqlite3_create_module(db, "vector_index", &vector_index_module, ctx);
}
