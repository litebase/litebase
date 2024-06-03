#include "./sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum
{
	PARAM_TYPE_INT,
	PARAM_TYPE_FLOAT,
	PARAM_TYPE_TEXT,
	PARAM_TYPE_NULL,
	PARAM_TYPE_BLOB
} ParamType;

typedef struct
{
	ParamType Type;
	int IntVal;
	double FloatVal;
	const char *TextVal;
	const void *BlobVal;
	int BlobLen;
} Parameter;

typedef struct
{
	char **column_names;
	void ***rows;
	int row_count;
	int column_count;
	int *column_types; // Store column types
} QueryResult;

QueryResult *execute_statement(sqlite3_stmt *stmt, Parameter *params, int param_count);

void free_query_result(QueryResult *result);
