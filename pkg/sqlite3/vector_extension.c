#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Forward declarations for CGO exports from pkg/vector
extern void *goEncodeVector(char *jsonStr, int *blobLen);
extern void goFreeVector(void *ptr);
extern long long goVectorScan(char *vfsID, char *databaseID, char *branchID, char *tableName, char *columnName, void *queryBlob, int queryBlobLen, int k, char *metric);
extern int goGetScanResult(long long handleID, long long *rowid, double *distance);
extern void goReleaseScanResults(long long handleID);

// ============================================================================
// vector_f32() Scalar Function
// ============================================================================

static void vector_f32_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_f32() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_f32() argument must be text", -1);
		return;
	}

	// Call Go function to parse and encode
	int blob_len = 0;
	void *blob = goEncodeVector((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	// Return BLOB (SQLite will make a copy)
	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);

	// Free Go-allocated memory
	goFreeVector(blob);
}

// ============================================================================
// vector_scan() Table-Valued Function
// ============================================================================

typedef struct vector_scan_vtab
{
	sqlite3_vtab base;
} vector_scan_vtab;

typedef struct vector_scan_cursor
{
	sqlite3_vtab_cursor base;
	long long scan_handle;
	long long current_rowid;
	double current_distance;
	int eof;
} vector_scan_cursor;

// xConnect - called when virtual table is first referenced
static int vector_scan_connect(
	sqlite3 *db,
	void *pAux,
	int argc,
	const char *const *argv,
	sqlite3_vtab **ppVTab,
	char **pzErr)
{
	vector_scan_vtab *pVTab = sqlite3_malloc(sizeof(vector_scan_vtab));

	if (pVTab == NULL)
	{
		return SQLITE_NOMEM;
	}

	memset(pVTab, 0, sizeof(vector_scan_vtab));

	// Declare the virtual table schema
	int rc = sqlite3_declare_vtab(db,
								  "CREATE TABLE x(rowid INTEGER, distance REAL)");

	if (rc != SQLITE_OK)
	{
		sqlite3_free(pVTab);
		return rc;
	}

	*ppVTab = (sqlite3_vtab *)pVTab;

	return SQLITE_OK;
}

// xDisconnect - called when virtual table is no longer referenced
static int vector_scan_disconnect(sqlite3_vtab *pVTab)
{
	sqlite3_free(pVTab);

	return SQLITE_OK;
}

// xBestIndex - query planner
static int vector_scan_best_index(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo)
{
	pInfo->estimatedCost = 1000.0;
	pInfo->estimatedRows = 10;

	return SQLITE_OK;
}

// xOpen - create new cursor
static int vector_scan_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
{
	vector_scan_cursor *pCur = sqlite3_malloc(sizeof(vector_scan_cursor));

	if (pCur == NULL)
	{
		return SQLITE_NOMEM;
	}

	memset(pCur, 0, sizeof(vector_scan_cursor));
	pCur->scan_handle = -1;
	pCur->eof = 1;

	*ppCursor = (sqlite3_vtab_cursor *)pCur;

	return SQLITE_OK;
}

// xClose - destroy cursor
static int vector_scan_close(sqlite3_vtab_cursor *pCursor)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;

	if (pCur->scan_handle >= 0)
	{
		goReleaseScanResults(pCur->scan_handle);
	}

	sqlite3_free(pCur);

	return SQLITE_OK;
}

// xFilter - begin scan
static int vector_scan_filter(
	sqlite3_vtab_cursor *pCursor,
	int idxNum,
	const char *idxStr,
	int argc,
	sqlite3_value **argv)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;

	// For now, use placeholder values for VFS context
	// TODO: Extract these from the connection context
	char *vfsID = "default";
	char *databaseID = "default";
	char *branchID = "main";

	// Arguments: table_name, column_name, query_vector, k, metric
	if (argc != 5)
	{
		return SQLITE_ERROR;
	}

	const char *table_name = (const char *)sqlite3_value_text(argv[0]);
	const char *column_name = (const char *)sqlite3_value_text(argv[1]);
	const void *query_blob = sqlite3_value_blob(argv[2]);
	int query_blob_len = sqlite3_value_bytes(argv[2]);
	int k = sqlite3_value_int(argv[3]);
	const char *metric = (const char *)sqlite3_value_text(argv[4]);

	if (metric == NULL)
	{
		metric = "l2";
	}

	// Call Go function to perform vector scan
	long long handle = goVectorScan(
		vfsID,
		databaseID,
		branchID,
		(char *)table_name,
		(char *)column_name,
		(void *)query_blob,
		query_blob_len,
		k,
		(char *)metric);

	if (handle <= 0)
	{
		pCursor->pVtab->zErrMsg = sqlite3_mprintf("Vector scan failed");
		return SQLITE_ERROR;
	}

	pCur->scan_handle = handle;

	// Get first result
	pCur->eof = !goGetScanResult(handle, &pCur->current_rowid, &pCur->current_distance);

	return SQLITE_OK;
}

// xNext - advance to next row
static int vector_scan_next(sqlite3_vtab_cursor *pCursor)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;

	// Get next result
	pCur->eof = !goGetScanResult(pCur->scan_handle, &pCur->current_rowid, &pCur->current_distance);

	return SQLITE_OK;
}

// xEof - check if at end of results
static int vector_scan_eof(sqlite3_vtab_cursor *pCursor)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;

	return pCur->eof;
}

// xColumn - return column value
static int vector_scan_column(
	sqlite3_vtab_cursor *pCursor,
	sqlite3_context *context,
	int column)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;

	if (column == 0)
	{
		// rowid column
		sqlite3_result_int64(context, pCur->current_rowid);
	}
	else
	{
		// distance column
		sqlite3_result_double(context, pCur->current_distance);
	}

	return SQLITE_OK;
}

// xRowid - return rowid
static int vector_scan_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
	vector_scan_cursor *pCur = (vector_scan_cursor *)pCursor;
	*pRowid = pCur->current_rowid;

	return SQLITE_OK;
}

// Virtual table module definition
static sqlite3_module vector_scan_module = {
	0,						/* iVersion */
	NULL,					/* xCreate */
	vector_scan_connect,	/* xConnect */
	vector_scan_best_index, /* xBestIndex */
	vector_scan_disconnect, /* xDisconnect */
	NULL,					/* xDestroy */
	vector_scan_open,		/* xOpen */
	vector_scan_close,		/* xClose */
	vector_scan_filter,		/* xFilter */
	vector_scan_next,		/* xNext */
	vector_scan_eof,		/* xEof */
	vector_scan_column,		/* xColumn */
	vector_scan_rowid,		/* xRowid */
	NULL,					/* xUpdate */
	NULL,					/* xBegin */
	NULL,					/* xSync */
	NULL,					/* xCommit */
	NULL,					/* xRollback */
	NULL,					/* xFindFunction */
	NULL,					/* xRename */
};

// Extension initialization
int sqlite3_vectorextension_init(
	sqlite3 *db,
	char **pzErrMsg,
	const sqlite3_api_routines *pApi)
{
	SQLITE_EXTENSION_INIT2(pApi);

	// Register vector_f32() scalar function
	int rc = sqlite3_create_function(
		db,
		"vector_f32",
		1,
		SQLITE_UTF8,
		NULL,
		vector_f32_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_scan virtual table
	rc = sqlite3_create_module(
		db,
		"vector_scan",
		&vector_scan_module,
		NULL);

	return rc;
}
