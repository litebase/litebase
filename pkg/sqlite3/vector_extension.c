#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Forward declarations for CGO exports from pkg/vector
extern void *goEncodeVector(char *jsonStr, int *blobLen);
extern void *goEncodeVectorF64(char *jsonStr, int *blobLen);
extern void *goEncodeVectorInt8(char *jsonStr, int *blobLen);
extern void *goEncodeVectorInt16(char *jsonStr, int *blobLen);
extern void *goEncodeVectorF16(char *jsonStr, int *blobLen);
extern void *goEncodeVectorBit(char *jsonStr, int *blobLen);
extern void *goEncodeVectorSparse(char *jsonStr, int *blobLen);
extern void goFreeVector(void *ptr);
extern long long goVectorScan(char *vfsID, char *databaseID, char *branchID, char *tableName, char *columnName, void *queryBlob, int queryBlobLen, int k, char *metric);
extern int goGetScanResult(long long handleID, long long *rowid, double *distance);
extern void goReleaseScanResults(long long handleID);
extern void *goQuantizeToInt8(void *blobPtr, int blobLen, int *resultLen, float *scaleOut, float *offsetOut);
extern void *goQuantizeToInt16(void *blobPtr, int blobLen, int *resultLen, float *scaleOut, float *offsetOut);
extern void *goQuantizeToFloat16(void *blobPtr, int blobLen, int *resultLen);
extern void *goQuantizeToBit(void *blobPtr, int blobLen, int *resultLen);
extern int goComputeHammingDistance(void *blobPtr1, int blobLen1, void *blobPtr2, int blobLen2);
extern long long goVectorSearch(char *vfsID, char *databaseID, char *branchID, char *tableName, char *columnName, void *queryBlob, int queryBlobLen, int k);
extern int goGetSearchResult(long long handleID, long long *rowid, double *distance);
extern void goReleaseSearchResults(long long handleID);
extern char *goVectorIndexStats(void *dbPtr, char *tableName);

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
// vector_f64() Scalar Function
// ============================================================================

static void vector_f64_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_f64() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_f64() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorF64((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_int8() Scalar Function
// ============================================================================

static void vector_int8_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_int8() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_int8() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorInt8((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_int16() Scalar Function
// ============================================================================

static void vector_int16_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_int16() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_int16() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorInt16((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_f16() Scalar Function
// ============================================================================

static void vector_f16_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_f16() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_f16() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorF16((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_bit() Scalar Function
// ============================================================================

static void vector_bit_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_bit() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_bit() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorBit((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_sparse() Scalar Function
// ============================================================================

static void vector_sparse_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_sparse() requires exactly 1 argument", -1);
		return;
	}

	const char *json_str = (const char *)sqlite3_value_text(argv[0]);

	if (json_str == NULL)
	{
		sqlite3_result_error(context, "vector_sparse() argument must be text", -1);
		return;
	}

	int blob_len = 0;
	void *blob = goEncodeVectorSparse((char *)json_str, &blob_len);

	if (blob == NULL || blob_len <= 0)
	{
		sqlite3_result_error(context, "Failed to parse vector JSON", -1);
		return;
	}

	sqlite3_result_blob(context, blob, blob_len, SQLITE_TRANSIENT);
	goFreeVector(blob);
}

// ============================================================================
// vector_quantize_int8() Scalar Function
// ============================================================================

static void vector_quantize_int8_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_quantize_int8() requires exactly 1 argument", -1);
		return;
	}

	const void *input_blob = sqlite3_value_blob(argv[0]);
	int input_len = sqlite3_value_bytes(argv[0]);

	if (input_blob == NULL || input_len == 0)
	{
		sqlite3_result_error(context, "vector_quantize_int8() argument must be a vector BLOB", -1);
		return;
	}

	int result_len = 0;
	float scale = 0.0f;
	float offset = 0.0f;
	void *result_blob = goQuantizeToInt8((void *)input_blob, input_len, &result_len, &scale, &offset);

	if (result_blob == NULL || result_len <= 0)
	{
		sqlite3_result_error(context, "Failed to quantize vector", -1);
		return;
	}

	// Return quantized BLOB
	sqlite3_result_blob(context, result_blob, result_len, SQLITE_TRANSIENT);

	// Store metadata in auxiliary data for retrieval
	// Note: In production, metadata should be stored alongside the BLOB
	sqlite3_set_auxdata(context, 0, &scale, NULL);
	sqlite3_set_auxdata(context, 1, &offset, NULL);

	goFreeVector(result_blob);
}

// ============================================================================
// vector_quantize_int16() Scalar Function
// ============================================================================

static void vector_quantize_int16_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_quantize_int16() requires exactly 1 argument", -1);
		return;
	}

	const void *input_blob = sqlite3_value_blob(argv[0]);
	int input_len = sqlite3_value_bytes(argv[0]);

	if (input_blob == NULL || input_len == 0)
	{
		sqlite3_result_error(context, "vector_quantize_int16() argument must be a vector BLOB", -1);
		return;
	}

	int result_len = 0;
	float scale = 0.0f;
	float offset = 0.0f;
	void *result_blob = goQuantizeToInt16((void *)input_blob, input_len, &result_len, &scale, &offset);

	if (result_blob == NULL || result_len <= 0)
	{
		sqlite3_result_error(context, "Failed to quantize vector", -1);
		return;
	}

	sqlite3_result_blob(context, result_blob, result_len, SQLITE_TRANSIENT);
	goFreeVector(result_blob);
}

// ============================================================================
// vector_quantize_f16() Scalar Function
// ============================================================================

static void vector_quantize_f16_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_quantize_f16() requires exactly 1 argument", -1);
		return;
	}

	const void *input_blob = sqlite3_value_blob(argv[0]);
	int input_len = sqlite3_value_bytes(argv[0]);

	if (input_blob == NULL || input_len == 0)
	{
		sqlite3_result_error(context, "vector_quantize_f16() argument must be a vector BLOB", -1);
		return;
	}

	int result_len = 0;
	void *result_blob = goQuantizeToFloat16((void *)input_blob, input_len, &result_len);

	if (result_blob == NULL || result_len <= 0)
	{
		sqlite3_result_error(context, "Failed to quantize vector", -1);
		return;
	}

	sqlite3_result_blob(context, result_blob, result_len, SQLITE_TRANSIENT);
	goFreeVector(result_blob);
}

// ============================================================================
// vector_quantize_bit() Scalar Function
// ============================================================================

static void vector_quantize_bit_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_quantize_bit() requires exactly 1 argument", -1);
		return;
	}

	const void *input_blob = sqlite3_value_blob(argv[0]);
	int input_len = sqlite3_value_bytes(argv[0]);

	if (input_blob == NULL || input_len == 0)
	{
		sqlite3_result_error(context, "vector_quantize_bit() argument must be a vector BLOB", -1);
		return;
	}

	int result_len = 0;
	void *result_blob = goQuantizeToBit((void *)input_blob, input_len, &result_len);

	if (result_blob == NULL || result_len <= 0)
	{
		sqlite3_result_error(context, "Failed to quantize vector", -1);
		return;
	}

	sqlite3_result_blob(context, result_blob, result_len, SQLITE_TRANSIENT);
	goFreeVector(result_blob);
}

// ============================================================================
// vector_scan() Table-Valued Function
// ============================================================================

// Context passed to vector_scan virtual table
typedef struct
{
	char *vfsID;
	char *databaseID;
	char *branchID;
} VectorScanContext;

// Context passed to vector_index virtual table
typedef struct
{
	char *vfsID;
	char *databaseID;
	char *branchID;
} VectorIndexContext;

typedef struct vector_scan_vtab
{
	sqlite3_vtab base;
	void *pAux; // Connection context
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
	pVTab->pAux = pAux; // Store connection context

	// Declare the virtual table schema with hidden parameter columns
	// Columns: rowid (0), distance (1), table_name (2 HIDDEN), column_name (3 HIDDEN), query_vector (4 HIDDEN), k (5 HIDDEN), metric (6 HIDDEN)
	int rc = sqlite3_declare_vtab(db,
								  "CREATE TABLE x(rowid INTEGER, distance REAL, "
								  "table_name TEXT HIDDEN, column_name TEXT HIDDEN, "
								  "query_vector BLOB HIDDEN, k INTEGER HIDDEN, metric TEXT HIDDEN)");

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
	// Hidden columns for parameters: table_name(2), column_name(3), query_vector(4), k(5), metric(6)
	int hasTable = 0, hasColumn = 0, hasQueryVector = 0, hasK = 0, hasMetric = 0;
	int argvIndex = 1;

	for (int i = 0; i < pInfo->nConstraint; i++)
	{
		if (pInfo->aConstraint[i].usable && pInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ)
		{
			switch (pInfo->aConstraint[i].iColumn)
			{
			case 2: // table_name
				hasTable = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 3: // column_name
				hasColumn = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 4: // query_vector
				hasQueryVector = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 5: // k
				hasK = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 6: // metric
				hasMetric = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			}
		}
	}

	// All 5 parameters are required
	if (!hasTable || !hasColumn || !hasQueryVector || !hasK || !hasMetric)
	{
		pVTab->zErrMsg = sqlite3_mprintf("vector_scan requires all parameters: table_name, column_name, query_vector, k, metric");
		return SQLITE_CONSTRAINT;
	}

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
	vector_scan_vtab *pVTab = (vector_scan_vtab *)pCursor->pVtab;

	// Get connection context
	VectorScanContext *ctx = (VectorScanContext *)pVTab->pAux;

	if (ctx == NULL || ctx->vfsID == NULL || ctx->databaseID == NULL || ctx->branchID == NULL)
	{
		pCursor->pVtab->zErrMsg = sqlite3_mprintf("vector_scan: missing connection context");
		return SQLITE_ERROR;
	}

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
		ctx->vfsID,
		ctx->databaseID,
		ctx->branchID,
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

// ============================================================================
// vector_hamming_distance() Scalar Function
// ============================================================================

static void vector_hamming_distance_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	if (argc != 2)
	{
		sqlite3_result_error(context, "vector_hamming_distance() requires exactly 2 arguments", -1);
		return;
	}

	const void *blob1 = sqlite3_value_blob(argv[0]);
	const void *blob2 = sqlite3_value_blob(argv[1]);

	if (blob1 == NULL || blob2 == NULL)
	{
		sqlite3_result_error(context, "vector_hamming_distance() arguments must be BLOBs", -1);
		return;
	}

	int blob1_len = sqlite3_value_bytes(argv[0]);
	int blob2_len = sqlite3_value_bytes(argv[1]);

	if (blob1_len == 0 || blob2_len == 0)
	{
		sqlite3_result_error(context, "vector_hamming_distance() arguments cannot be empty", -1);
		return;
	}

	// Call Go function to compute Hamming distance
	int distance = goComputeHammingDistance(
		(void *)blob1, blob1_len,
		(void *)blob2, blob2_len);

	if (distance < 0)
	{
		sqlite3_result_error(context, "Failed to compute Hamming distance", -1);
		return;
	}

	sqlite3_result_int(context, distance);
}

// ============================================================================
// vector_search Virtual Table - ANN search using clustered index
// ============================================================================

// Virtual table and cursor structures (same as vector_scan)
typedef struct vector_search_vtab
{
	sqlite3_vtab base;
	void *pAux; // Connection context
} vector_search_vtab;

typedef struct vector_search_cursor
{
	sqlite3_vtab_cursor base;
	long long search_handle;
	long long current_rowid;
	double current_distance;
	int eof;
} vector_search_cursor;

// xConnect - called when virtual table is first referenced
static int vector_search_connect(
	sqlite3 *db,
	void *pAux,
	int argc,
	const char *const *argv,
	sqlite3_vtab **ppVTab,
	char **pzErr)
{
	vector_search_vtab *pVTab = sqlite3_malloc(sizeof(vector_search_vtab));

	if (pVTab == NULL)
	{
		return SQLITE_NOMEM;
	}

	memset(pVTab, 0, sizeof(vector_search_vtab));
	pVTab->pAux = pAux; // Store connection context

	// Declare the virtual table schema with hidden parameter columns
	// Columns: rowid (0), distance (1), table_name (2 HIDDEN), column_name (3 HIDDEN), query_vector (4 HIDDEN), k (5 HIDDEN), metric (6 HIDDEN)
	int rc = sqlite3_declare_vtab(db,
								  "CREATE TABLE x(rowid INTEGER, distance REAL, "
								  "table_name TEXT HIDDEN, column_name TEXT HIDDEN, "
								  "query_vector BLOB HIDDEN, k INTEGER HIDDEN, metric TEXT HIDDEN)");

	if (rc != SQLITE_OK)
	{
		sqlite3_free(pVTab);
		return rc;
	}

	*ppVTab = (sqlite3_vtab *)pVTab;

	return SQLITE_OK;
}

// xDisconnect - called when virtual table is no longer referenced
static int vector_search_disconnect(sqlite3_vtab *pVTab)
{
	sqlite3_free(pVTab);

	return SQLITE_OK;
}

// xBestIndex - query planner
static int vector_search_best_index(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo)
{
	// Hidden columns for parameters: table_name(2), column_name(3), query_vector(4), k(5)
	// Note: metric is read from table metadata
	int hasTable = 0, hasColumn = 0, hasQueryVector = 0, hasK = 0;
	int argvIndex = 1;

	for (int i = 0; i < pInfo->nConstraint; i++)
	{
		if (pInfo->aConstraint[i].usable && pInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ)
		{
			switch (pInfo->aConstraint[i].iColumn)
			{
			case 2: // table_name
				hasTable = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 3: // column_name
				hasColumn = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 4: // query_vector
				hasQueryVector = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			case 5: // k
				hasK = 1;
				pInfo->aConstraintUsage[i].argvIndex = argvIndex++;
				pInfo->aConstraintUsage[i].omit = 1;
				break;
			}
		}
	}

	// All 4 parameters are required
	if (!hasTable || !hasColumn || !hasQueryVector || !hasK)
	{
		pVTab->zErrMsg = sqlite3_mprintf("vector_search requires all parameters: table_name, column_name, query_vector, k");
		return SQLITE_CONSTRAINT;
	}

	pInfo->estimatedCost = 100.0; // Lower cost than vector_scan since it's approximate
	pInfo->estimatedRows = 10;

	return SQLITE_OK;
}

// xOpen - create new cursor
static int vector_search_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
{
	vector_search_cursor *pCur = sqlite3_malloc(sizeof(vector_search_cursor));

	if (pCur == NULL)
	{
		return SQLITE_NOMEM;
	}

	memset(pCur, 0, sizeof(vector_search_cursor));
	pCur->search_handle = -1;
	pCur->eof = 1;

	*ppCursor = (sqlite3_vtab_cursor *)pCur;

	return SQLITE_OK;
}

// xClose - destroy cursor
static int vector_search_close(sqlite3_vtab_cursor *pCursor)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;

	if (pCur->search_handle >= 0)
	{
		goReleaseSearchResults(pCur->search_handle);
	}

	sqlite3_free(pCur);

	return SQLITE_OK;
}

// xFilter - begin search (using cluster-based ANN)
static int vector_search_filter(
	sqlite3_vtab_cursor *pCursor,
	int idxNum,
	const char *idxStr,
	int argc,
	sqlite3_value **argv)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;
	vector_search_vtab *pVTab = (vector_search_vtab *)pCursor->pVtab;

	// Get connection context
	VectorScanContext *ctx = (VectorScanContext *)pVTab->pAux;

	if (ctx == NULL || ctx->vfsID == NULL || ctx->databaseID == NULL || ctx->branchID == NULL)
	{
		pCursor->pVtab->zErrMsg = sqlite3_mprintf("vector_search: missing connection context");
		return SQLITE_ERROR;
	}

	// Arguments: table_name, column_name, query_vector, k
	// Note: metric is read from table metadata by the Go function
	if (argc != 4)
	{
		return SQLITE_ERROR;
	}

	const char *table_name = (const char *)sqlite3_value_text(argv[0]);
	const char *column_name = (const char *)sqlite3_value_text(argv[1]);
	const void *query_blob = sqlite3_value_blob(argv[2]);
	int query_blob_len = sqlite3_value_bytes(argv[2]);
	int k = sqlite3_value_int(argv[3]);

	// Call Go function to perform cluster-based search (ANN)
	// The metric is read from the table's metadata
	long long handle = goVectorSearch(
		ctx->vfsID,
		ctx->databaseID,
		ctx->branchID,
		(char *)table_name,
		(char *)column_name,
		(void *)query_blob,
		query_blob_len,
		k);

	if (handle < 0)
	{
		pCursor->pVtab->zErrMsg = sqlite3_mprintf("Vector search failed");
		return SQLITE_ERROR;
	}

	pCur->search_handle = handle;

	// Get first result
	pCur->eof = !goGetSearchResult(handle, &pCur->current_rowid, &pCur->current_distance);

	return SQLITE_OK;
}

// xNext - advance to next row
static int vector_search_next(sqlite3_vtab_cursor *pCursor)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;

	// Get next result
	pCur->eof = !goGetSearchResult(pCur->search_handle, &pCur->current_rowid, &pCur->current_distance);

	return SQLITE_OK;
}

// xEof - check if at end of results
static int vector_search_eof(sqlite3_vtab_cursor *pCursor)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;

	return pCur->eof;
}

// xColumn - return column value
static int vector_search_column(
	sqlite3_vtab_cursor *pCursor,
	sqlite3_context *context,
	int column)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;

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
static int vector_search_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
	vector_search_cursor *pCur = (vector_search_cursor *)pCursor;
	*pRowid = pCur->current_rowid;

	return SQLITE_OK;
}

// ============================================================================
// vector_index_stats() Scalar Function - Returns JSON stats for an index
// ============================================================================

static void vector_index_stats_func(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv)
{
	// vector_index_stats(table_name)
	if (argc != 1)
	{
		sqlite3_result_error(context, "vector_index_stats() requires 1 argument: table_name", -1);
		return;
	}

	const char *table_name = (const char *)sqlite3_value_text(argv[0]);

	if (table_name == NULL)
	{
		sqlite3_result_error(context, "table_name must be text", -1);
		return;
	}

	// Get database connection
	sqlite3 *db = sqlite3_context_db_handle(context);

	// Call Go function to get stats
	char *stats_json = goVectorIndexStats(db, (char *)table_name);

	if (stats_json == NULL)
	{
		sqlite3_result_error(context, "Failed to retrieve index statistics", -1);
		return;
	}

	// Return JSON result
	sqlite3_result_text(context, stats_json, -1, SQLITE_TRANSIENT);
	free(stats_json);
}

// Virtual table module definition for vector_scan (brute-force k-NN)
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
	NULL,					/* xSavepoint */
	NULL,					/* xRelease */
	NULL					/* xRollbackTo */
};

// Virtual table module definition for vector_search (ANN using clustered index)
static sqlite3_module vector_search_module = {
	0,						  /* iVersion */
	NULL,					  /* xCreate */
	vector_search_connect,	  /* xConnect */
	vector_search_best_index, /* xBestIndex */
	vector_search_disconnect, /* xDisconnect */
	NULL,					  /* xDestroy */
	vector_search_open,		  /* xOpen */
	vector_search_close,	  /* xClose */
	vector_search_filter,	  /* xFilter */
	vector_search_next,		  /* xNext */
	vector_search_eof,		  /* xEof */
	vector_search_column,	  /* xColumn */
	vector_search_rowid,	  /* xRowid */
	NULL,					  /* xUpdate */
	NULL,					  /* xBegin */
	NULL,					  /* xSync */
	NULL,					  /* xCommit */
	NULL,					  /* xRollback */
	NULL,					  /* xFindFunction */
	NULL,					  /* xRename */
	NULL,					  /* xSavepoint */
	NULL,					  /* xRelease */
	NULL					  /* xRollbackTo */
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

	// Register vector_f64() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_f64",
		1,
		SQLITE_UTF8,
		NULL,
		vector_f64_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_int8() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_int8",
		1,
		SQLITE_UTF8,
		NULL,
		vector_int8_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_int16() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_int16",
		1,
		SQLITE_UTF8,
		NULL,
		vector_int16_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_f16() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_f16",
		1,
		SQLITE_UTF8,
		NULL,
		vector_f16_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_bit() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_bit",
		1,
		SQLITE_UTF8,
		NULL,
		vector_bit_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_sparse() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_sparse",
		1,
		SQLITE_UTF8,
		NULL,
		vector_sparse_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_quantize_int8() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_quantize_int8",
		1,
		SQLITE_UTF8,
		NULL,
		vector_quantize_int8_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_quantize_int16() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_quantize_int16",
		1,
		SQLITE_UTF8,
		NULL,
		vector_quantize_int16_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_quantize_f16() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_quantize_f16",
		1,
		SQLITE_UTF8,
		NULL,
		vector_quantize_f16_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_quantize_bit() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_quantize_bit",
		1,
		SQLITE_UTF8,
		NULL,
		vector_quantize_bit_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_index_stats() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_index_stats",
		1,
		SQLITE_UTF8,
		NULL,
		vector_index_stats_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_hamming_distance() scalar function
	rc = sqlite3_create_function(
		db,
		"vector_hamming_distance",
		2,
		SQLITE_UTF8,
		NULL,
		vector_hamming_distance_func,
		NULL,
		NULL);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Note: vector_search() and vector_scan() function registration happens per-connection
	// in sqlite3_register_vector_scan() since they need connection context

	// Register vector_scan virtual table
	rc = sqlite3_create_module(
		db,
		"vector_scan",
		&vector_scan_module,
		NULL);

	return rc;
}

// ============================================================================
// sqlite3_register_vector_scan - Per-connection registration
// ============================================================================

// Size of VectorScanContext struct for Go to allocate
const int sizeof_VectorScanContext = sizeof(VectorScanContext);

// Size of VectorIndexContext struct for Go to allocate
const int sizeof_VectorIndexContext = sizeof(VectorIndexContext);

// Function destructor to free context
static void vector_scan_context_destructor(void *pCtx)
{
	if (pCtx != NULL)
	{
		VectorScanContext *ctx = (VectorScanContext *)pCtx;

		if (ctx->vfsID != NULL)
		{
			free(ctx->vfsID);
		}

		if (ctx->databaseID != NULL)
		{
			free(ctx->databaseID);
		}

		if (ctx->branchID != NULL)
		{
			free(ctx->branchID);
		}

		sqlite3_free(ctx);
	}
}

// Register vector_scan virtual table for a specific connection with context
int sqlite3_register_vector_scan(
	sqlite3 *db,
	void *ctx_ptr,
	char *vfsID,
	char *databaseID,
	char *branchID)
{
	int rc;
	VectorScanContext *ctx = (VectorScanContext *)ctx_ptr;

	// Set context fields (strings are already allocated by Go)
	ctx->vfsID = vfsID;
	ctx->databaseID = databaseID;
	ctx->branchID = branchID;

	// Register the virtual table module with the context
	// The module will use this context for all table instances
	rc = sqlite3_create_module_v2(
		db,
		"vector_scan",
		&vector_scan_module,
		ctx,
		NULL); // Don't use destructor here since context is shared

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register vector_search virtual table module (ANN using clustered index)
	rc = sqlite3_create_module_v2(
		db,
		"vector_search",
		&vector_search_module,
		ctx,
		vector_scan_context_destructor); // Destructor only on last module registration

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	// Register the vector_index module (defined in pkg/vector/virtual_table_index.go)
	extern int sqlite3_register_vector_index(sqlite3 * db);
	rc = sqlite3_register_vector_index(db);

	if (rc != SQLITE_OK)
	{
		return rc;
	}

	return SQLITE_OK;
}
