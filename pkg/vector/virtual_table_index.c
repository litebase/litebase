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
    char *name;     // Column name
    char *type;     // Column type (INTEGER, TEXT, BLOB, REAL, etc.)
    int affinity;   // SQLite type affinity (SQLITE_INTEGER, SQLITE_TEXT, etc.)
    int is_vector;  // 1 if this is a vector column (BLOB type), 0 otherwise
    int dimensions; // For vector columns: number of dimensions (0 for non-vector columns)
} ColumnDef;

// Buffer for batching inserts
// TODO: Migrate to full column_values array for multi-vector and user-defined column support
typedef struct
{
    void *vector_data; // Temporary: single vector for backward compatibility
    int vector_size;   // Temporary: vector size in bytes
    sqlite3_int64 created_at;
    // Future: sqlite3_value **column_values; // Array of values for all columns
    // Future: int num_columns;               // Number of columns
} PendingInsert;

// SQLite has a parameter limit of 32766 (SQLITE_MAX_VARIABLE_NUMBER)
// The limiting factor is _cluster_vector_map with 3 params per row (vector_id, cluster_id, created_at)
// So max batch size = 32766 / 3 = 10922 vectors
#define INSERT_BUFFER_CAPACITY 10922

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

// Prepare cached statements for a vtab (helper)
static int prepare_vtab_statements(vector_index_vtab *vtab, char **pzErr)
{
    char *sql = NULL;
    int rc;

    // insert_vector_stmt - build dynamically based on columns
    // Format: INSERT INTO table_vectors (col1, col2, ..., created_at) VALUES (?1, ?2, ..., ?N)
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
        "INSERT INTO %s_vectors (%s, created_at) VALUES (%s, ?%d)",
        vtab->table_name, col_list, param_list, vtab->num_columns + 1);

    sqlite3_free(col_list);
    sqlite3_free(param_list);

    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &vtab->insert_vector_stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare insert_vector_stmt: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // upsert_map_stmt (insert or update mapping)
    sql = sqlite3_mprintf(
        "INSERT INTO %s_cluster_vector_map (vector_id, cluster_id, indexed_at) VALUES (?1, ?2, ?3) "
        "ON CONFLICT(vector_id) DO UPDATE SET cluster_id=excluded.cluster_id, indexed_at=excluded.indexed_at",
        vtab->table_name);
    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &vtab->upsert_map_stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare upsert_map_stmt: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // delete_map_stmt
    sql = sqlite3_mprintf(
        "DELETE FROM %s_cluster_vector_map WHERE vector_id = ?1",
        vtab->table_name);
    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &vtab->delete_map_stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare delete_map_stmt: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // inc_cluster_size_stmt
    sql = sqlite3_mprintf(
        "UPDATE %s_cluster_tree SET cluster_size = cluster_size + 1 WHERE cluster_id = 0",
        vtab->table_name);
    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &vtab->inc_cluster_size_stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare inc_cluster_size_stmt: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

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

    char *final_defs = sqlite3_mprintf("%s, created_at INTEGER NOT NULL", column_defs);
    sqlite3_free(column_defs);
    column_defs = final_defs;

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

    // Create _cluster_tree table: Hierarchical cluster structure
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_cluster_tree ("
        "cluster_id INTEGER PRIMARY KEY,"
        "parent_id INTEGER DEFAULT NULL,"
        "centroid_blob BLOB NOT NULL,"
        "is_leaf INTEGER NOT NULL DEFAULT 1,"
        "cluster_size INTEGER DEFAULT 0,"
        "radius REAL DEFAULT 0.0,"
        "created_at INTEGER NOT NULL"
        ")",
        table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create cluster_tree table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create index on parent_id for tree traversal
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_cluster_tree_parent_idx ON %s_cluster_tree(parent_id)",
        table_name, table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create cluster_tree parent index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create index on is_leaf for finding leaf clusters
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_cluster_tree_leaf_idx ON %s_cluster_tree(is_leaf) WHERE is_leaf = 1",
        table_name, table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create cluster_tree leaf index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create _cluster_vector_map table: Skinny mapping table (avoids updating large vector rows)
    sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS %s_cluster_vector_map ("
        "vector_id INTEGER NOT NULL,"
        "cluster_id INTEGER NOT NULL,"
        "distance REAL,"
        "indexed_at INTEGER NOT NULL,"
        "PRIMARY KEY (vector_id)"
        ")",
        table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create cluster_vector_map table: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Create B-tree index on cluster_id for fast retrieval of cluster members
    sql = sqlite3_mprintf(
        "CREATE INDEX IF NOT EXISTS %s_cluster_vector_map_cluster_idx ON %s_cluster_vector_map(cluster_id)",
        table_name, table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create cluster_vector_map cluster index: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
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
        columns[col_count].dimensions = 0; // Will be set later if specified

        col_count++;
    }

    // Second pass: apply default dimensions to vector columns that don't have specific dimensions
    for (int i = 0; i < col_count; i++)
    {
        if (columns[i].is_vector && columns[i].dimensions == 0)
        {
            columns[i].dimensions = default_dimensions;
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

    // Store metadata (using first vector column's dimensions for backward compatibility)
    // Also store the first vector column name for dynamic SQL generation in Go code
    const char *first_vector_col = NULL;

    for (int i = 0; i < num_columns; i++)
    {
        if (columns[i].is_vector)
        {
            first_vector_col = columns[i].name;
            break;
        }
    }

    if (!first_vector_col)
    {
        *pzErr = sqlite3_mprintf("No vector column found in column definitions");
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return SQLITE_ERROR;
    }

    char *sql = sqlite3_mprintf(
        "INSERT OR REPLACE INTO %s_metadata (key, value) VALUES "
        "('dimensions', '%d'), "
        "('distance_metric', '%d'), "
        "('max_cluster_size', '%d'), "
        "('min_cluster_size', '%d'), "
        "('pending_count', '0'), "
        "('last_indexed_at', '0'), "
        "('vector_column', '%q')",
        vtab->table_name,
        vtab->dimensions,
        distance_metric,
        max_cluster_size,
        min_cluster_size,
        first_vector_col);
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

    // Create root cluster (cluster 0) for fast initial assignments
    // All vectors start in cluster 0, then background job reassigns to proper clusters
    sql = sqlite3_mprintf(
        "INSERT OR IGNORE INTO %s_cluster_tree (cluster_id, parent_id, centroid_blob, is_leaf, cluster_size, created_at) "
        "SELECT 0, NULL, X'00000000', 1, 0, %lld "
        "WHERE NOT EXISTS (SELECT 1 FROM %s_cluster_tree WHERE cluster_id = 0)",
        vtab->table_name, (long long)time(NULL), vtab->table_name);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to create root cluster: %s", err_msg);
        sqlite3_free(err_msg);
        sqlite3_free(vtab->table_name);
        sqlite3_free(vtab);
        return rc;
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

    // Load metadata
    char *sql = sqlite3_mprintf(
        "SELECT value FROM %s_metadata WHERE key = 'dimensions' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'distance_metric' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'max_cluster_size' "
        "UNION ALL SELECT value FROM %s_metadata WHERE key = 'min_cluster_size'",
        vtab->table_name, vtab->table_name, vtab->table_name, vtab->table_name);

    sqlite3_stmt *stmt;
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
        switch (row)
        {
        case 0:
            vtab->dimensions = atoi(value);
            break;
        case 1:
            vtab->distance_metric = atoi(value);
            break;
        case 2:
            vtab->max_cluster_size = atoi(value);
            break;
        case 3:
            vtab->min_cluster_size = atoi(value);
            break;
        }
        row++;
    }
    sqlite3_finalize(stmt);

    // Declare virtual table schema
    rc = sqlite3_declare_vtab(db, "CREATE TABLE x(id INTEGER PRIMARY KEY, vector BLOB)");
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
            if (vtab->insert_buffer[i].vector_data)
            {
                sqlite3_free(vtab->insert_buffer[i].vector_data);
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

    // Build multi-row INSERT for _vectors table
    // For now: INSERT using first vector column only (backward compatible)
    // Future: Support all columns from column_values array

    // Find first vector column name
    const char *vector_col_name = "vector"; // default
    for (int i = 0; i < vtab->num_columns; i++)
    {
        if (vtab->columns[i].is_vector)
        {
            vector_col_name = vtab->columns[i].name;
            break;
        }
    }

    int param_count = vtab->buffer_size * 2; // 2 params per row (vector + created_at)

    // Build VALUES clause - allocate enough space
    // Each (?,?) is 5 chars, plus comma = 6 chars per row (except first)
    int values_size = vtab->buffer_size * 10; // Conservative estimate
    char *values_clause = (char *)sqlite3_malloc(values_size);
    if (!values_clause)
    {
        *pzErr = sqlite3_mprintf("Out of memory building INSERT statement");
        return SQLITE_NOMEM;
    }

    char *ptr = values_clause;
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (i > 0)
        {
            ptr += sprintf(ptr, ",");
        }
        ptr += sprintf(ptr, "(?,?)");
    }

    sql = sqlite3_mprintf(
        "INSERT INTO %s_vectors (%s, created_at) VALUES %s",
        vtab->table_name, vector_col_name, values_clause);
    sqlite3_free(values_clause);

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Failed to prepare batch insert: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // Bind all parameters
    int param_idx = 1;
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        sqlite3_bind_blob(stmt, param_idx++,
                          vtab->insert_buffer[i].vector_data,
                          vtab->insert_buffer[i].vector_size,
                          SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, param_idx++, vtab->insert_buffer[i].created_at);
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE)
    {
        *pzErr = sqlite3_mprintf("Batch insert failed: %s", sqlite3_errmsg(vtab->db));
        return rc;
    }

    // Get the first and last inserted rowids
    // For multi-row INSERT, last_insert_rowid() should give us the first rowid
    sqlite3_int64 first_rowid = sqlite3_last_insert_rowid(vtab->db);

    // Verify by getting the max ID (last inserted row)
    // This is more reliable than assuming sequential IDs
    sql = sqlite3_mprintf("SELECT MAX(id) FROM %s_vectors", vtab->table_name);
    sqlite3_stmt *max_stmt;
    rc = sqlite3_prepare_v2(vtab->db, sql, -1, &max_stmt, NULL);
    sqlite3_free(sql);
    if (rc == SQLITE_OK)
    {
        if (sqlite3_step(max_stmt) == SQLITE_ROW)
        {
            sqlite3_int64 last_rowid = sqlite3_column_int64(max_stmt, 0);
            // Recalculate first_rowid from last_rowid
            first_rowid = last_rowid - vtab->buffer_size + 1;
        }
        sqlite3_finalize(max_stmt);
    }

    // Build multi-row INSERT for _cluster_vector_map
    // INSERT INTO table_cluster_vector_map (vector_id, cluster_id, indexed_at) VALUES (?,0,?),(?,0,?),...
    // Each row is approximately: (vector_id,cluster_id,timestamp) = ~30 chars worst case
    // Plus comma separator = ~31 chars per row
    // Allocate 40 bytes per row to be safe
    values_clause = (char *)sqlite3_malloc(vtab->buffer_size * 40 + 1);
    if (!values_clause)
    {
        *pzErr = sqlite3_mprintf("Out of memory building map INSERT");
        return SQLITE_NOMEM;
    }

    ptr = values_clause;
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (i > 0)
        {
            ptr += sprintf(ptr, ",");
        }
        ptr += sprintf(ptr, "(%lld,0,%lld)",
                       (long long)(first_rowid + i),
                       (long long)vtab->insert_buffer[i].created_at);
    }
    *ptr = '\0'; // Ensure null termination

    sql = sqlite3_mprintf(
        "INSERT INTO %s_cluster_vector_map (vector_id, cluster_id, indexed_at) VALUES %s",
        vtab->table_name, values_clause);
    sqlite3_free(values_clause);

    rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Batch map insert failed: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Update cluster_size once for entire batch
    sql = sqlite3_mprintf(
        "UPDATE %s_cluster_tree SET cluster_size = cluster_size + %d WHERE cluster_id = 0",
        vtab->table_name, vtab->buffer_size);
    rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql);
    if (rc != SQLITE_OK)
    {
        *pzErr = sqlite3_mprintf("Batch cluster_size update failed: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // Notify Go once for the entire batch (not per vector)
    VectorIndexContext *ctx = (VectorIndexContext *)vtab->pAux;
    if (ctx && ctx->databaseID && ctx->branchID)
    {
        goNotifyVectorInsert(ctx->databaseID, ctx->branchID, vtab->table_name);
    }

    // Free buffered vector data
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        sqlite3_free(vtab->insert_buffer[i].vector_data);
        vtab->insert_buffer[i].vector_data = NULL;
    }

    vtab->buffer_size = 0;
    return SQLITE_OK;
}

// xBegin: Start a transaction
int vector_index_begin(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;
    vtab->in_transaction = 1;
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

    vtab->in_transaction = 0;
    return SQLITE_OK;
}

// xRollback: Rollback transaction (clear buffer without flushing)
int vector_index_rollback(sqlite3_vtab *pVtab)
{
    vector_index_vtab *vtab = (vector_index_vtab *)pVtab;

    // Free buffered vector data without flushing
    for (int i = 0; i < vtab->buffer_size; i++)
    {
        if (vtab->insert_buffer[i].vector_data)
        {
            sqlite3_free(vtab->insert_buffer[i].vector_data);
            vtab->insert_buffer[i].vector_data = NULL;
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
    // Use first vector column name dynamically
    const char *vector_col_name = "vector"; // default
    for (int i = 0; i < vtab->num_columns; i++)
    {
        if (vtab->columns[i].is_vector)
        {
            vector_col_name = vtab->columns[i].name;
            break;
        }
    }

    char *sql = sqlite3_mprintf(
        "SELECT v.id, v.%s FROM %s_vectors v "
        "INNER JOIN %s_cluster_vector_map m ON v.id = m.vector_id",
        vector_col_name, vtab->table_name, vtab->table_name);

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

        // Use prepared delete statement
        if (vtab->delete_map_stmt)
        {
            sqlite3_bind_int64(vtab->delete_map_stmt, 1, vector_id);
            rc = sqlite3_step(vtab->delete_map_stmt);
            sqlite3_reset(vtab->delete_map_stmt);
            sqlite3_clear_bindings(vtab->delete_map_stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("DELETE failed: %s", sqlite3_errmsg(vtab->db));
                return rc;
            }
        }
        else
        {
            // Fallback to exec
            sql = sqlite3_mprintf(
                "DELETE FROM %s_cluster_vector_map WHERE vector_id = %lld",
                vtab->table_name, vector_id);

            rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
            sqlite3_free(sql);
            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("DELETE failed: %s", err_msg);
                sqlite3_free(err_msg);
                return rc;
            }
        }
    }
    else if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL)
    {
        // INSERT: argv[0] = NULL, argv[1] = new rowid (might be NULL for auto-rowid), argv[2] = id, argv[3] = vector
        const void *vector_data = sqlite3_value_blob(argv[3]);
        int vector_size = sqlite3_value_bytes(argv[3]);

        // Buffer the insert instead of executing immediately
        // This allows batching multiple inserts into a single multi-row INSERT statement

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

        // Add to buffer (make a copy of vector data since it may be freed by SQLite)
        void *vector_copy = sqlite3_malloc(vector_size);
        if (!vector_copy)
        {
            pVtab->zErrMsg = sqlite3_mprintf("Out of memory buffering insert");
            return SQLITE_NOMEM;
        }
        memcpy(vector_copy, vector_data, vector_size);

        vtab->insert_buffer[vtab->buffer_size].vector_data = vector_copy;
        vtab->insert_buffer[vtab->buffer_size].vector_size = vector_size;
        vtab->insert_buffer[vtab->buffer_size].created_at = (sqlite3_int64)time(NULL);
        vtab->buffer_size++;

        // Set pRowid to a temporary value (will be corrected on flush)
        // SQLite requires us to set *something* here
        *pRowid = 0;
        sql = NULL;
    }
    else if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL && sqlite3_value_type(argv[1]) != SQLITE_NULL)
    {
        // UPDATE: argv[0] = old rowid, argv[1] = new rowid, argv[2+] = columns
        sqlite3_int64 old_vector_id = sqlite3_value_int64(argv[0]);
        const void *vector_data = sqlite3_value_blob(argv[3]);
        int vector_size = sqlite3_value_bytes(argv[3]);

        // Delete old mapping entry using prepared statement
        if (vtab->delete_map_stmt)
        {
            sqlite3_bind_int64(vtab->delete_map_stmt, 1, old_vector_id);
            rc = sqlite3_step(vtab->delete_map_stmt);
            sqlite3_reset(vtab->delete_map_stmt);
            sqlite3_clear_bindings(vtab->delete_map_stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE delete mapping failed: %s", sqlite3_errmsg(vtab->db));
                return rc;
            }
        }
        else
        {
            sql = sqlite3_mprintf(
                "DELETE FROM %s_cluster_vector_map WHERE vector_id = %lld",
                vtab->table_name, old_vector_id);
            rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
            sqlite3_free(sql);
            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE delete mapping failed: %s", err_msg);
                sqlite3_free(err_msg);
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
            "INSERT INTO %s_vectors (%s, created_at) VALUES (?1, ?2)",
            vtab->table_name, vector_col_name);

        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(vtab->db, sql, -1, &stmt, NULL);
        sqlite3_free(sql);
        if (rc != SQLITE_OK)
            return rc;

        sqlite3_bind_blob(stmt, 1, vector_data, vector_size, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)time(NULL));

        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE)
            return rc;

        sqlite3_int64 new_vector_id = sqlite3_last_insert_rowid(vtab->db);

        // Assign new vector to cluster 0 using upsert
        if (vtab->upsert_map_stmt)
        {
            sqlite3_bind_int64(vtab->upsert_map_stmt, 1, new_vector_id);
            sqlite3_bind_int64(vtab->upsert_map_stmt, 2, 0);
            sqlite3_bind_int64(vtab->upsert_map_stmt, 3, (sqlite3_int64)time(NULL));

            rc = sqlite3_step(vtab->upsert_map_stmt);
            sqlite3_reset(vtab->upsert_map_stmt);
            sqlite3_clear_bindings(vtab->upsert_map_stmt);

            if (rc != SQLITE_DONE && rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE assign to cluster failed: %s", sqlite3_errmsg(vtab->db));
                return rc;
            }
        }
        else
        {
            sql = sqlite3_mprintf(
                "INSERT INTO %s_cluster_vector_map (vector_id, cluster_id, indexed_at) VALUES (%lld, 0, %lld)",
                vtab->table_name, new_vector_id, (sqlite3_int64)time(NULL));
            rc = sqlite3_exec(vtab->db, sql, NULL, NULL, &err_msg);
            sqlite3_free(sql);
            if (rc != SQLITE_OK)
            {
                pVtab->zErrMsg = sqlite3_mprintf("UPDATE assign to cluster failed: %s", err_msg);
                sqlite3_free(err_msg);
                return rc;
            }
        }

        // Notify for background reassignment
        VectorIndexContext *ctx = (VectorIndexContext *)vtab->pAux;
        if (ctx)
        {
            goNotifyVectorInsert(ctx->databaseID, ctx->branchID, vtab->table_name);
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
