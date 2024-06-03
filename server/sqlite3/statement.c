#include "./statement.h"

// Function to execute the query
QueryResult *execute_statement(sqlite3_stmt *stmt, Parameter *params, int param_count)
{
	QueryResult *result = (QueryResult *)malloc(sizeof(QueryResult));
	int rc, i, j;

	// Initialize result
	result->column_names = NULL;
	result->rows = NULL;
	result->row_count = 0;
	result->column_count = 0;
	result->column_types = NULL;

	// Bind parameters
	for (i = 0; i < param_count; i++)
	{
		switch (params[i].Type)
		{
		case PARAM_TYPE_INT:
			sqlite3_bind_int(stmt, i + 1, params[i].IntVal);
			break;
		case PARAM_TYPE_FLOAT:
			sqlite3_bind_double(stmt, i + 1, params[i].FloatVal);
			break;
		case PARAM_TYPE_TEXT:
			sqlite3_bind_text(stmt, i + 1, params[i].TextVal, -1, SQLITE_STATIC);
			break;
		case PARAM_TYPE_NULL:
			sqlite3_bind_null(stmt, i + 1);
			break;
		case PARAM_TYPE_BLOB:
			sqlite3_bind_blob(stmt, i + 1, params[i].BlobVal, params[i].BlobLen, SQLITE_STATIC);
			break;
		}
	}

	// Get column count
	result->column_count = sqlite3_column_count(stmt);

	// Allocate memory for column names
	result->column_names = (char **)malloc(result->column_count * sizeof(char *));
	for (i = 0; i < result->column_count; i++)
	{
		result->column_names[i] = strdup(sqlite3_column_name(stmt, i));
		// result->column_types[i] = sqlite3_column_type(stmt, i);
	}

	// Execute the query and collect results
	result->rows = (void ***)malloc(sizeof(void **));

	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		if (result->column_types == NULL)
		{
			result->column_types = (int *)malloc(result->column_count * sizeof(int));

			for (i = 0; i < result->column_count; i++)
			{
				result->column_types[i] = sqlite3_column_type(stmt, i);
			}
		}

		result->row_count++;
		result->rows = (void ***)realloc(result->rows, result->row_count * sizeof(void **));
		result->rows[result->row_count - 1] = (void **)malloc(result->column_count * sizeof(void *));
		for (i = 0; i < result->column_count; i++)
		{
			switch (result->column_types[i])
			{
			case SQLITE_INTEGER:
				result->rows[result->row_count - 1][i] = malloc(sizeof(int));
				*(int *)result->rows[result->row_count - 1][i] = sqlite3_column_int(stmt, i);
				break;
			case SQLITE_FLOAT:
				result->rows[result->row_count - 1][i] = malloc(sizeof(double));
				*(double *)result->rows[result->row_count - 1][i] = sqlite3_column_double(stmt, i);
				break;
			case SQLITE_TEXT:
				result->rows[result->row_count - 1][i] = strdup((const char *)sqlite3_column_text(stmt, i));
				break;
			case SQLITE_BLOB:
			{
				int blob_len = sqlite3_column_bytes(stmt, i);
				result->rows[result->row_count - 1][i] = malloc(blob_len);
				memcpy(result->rows[result->row_count - 1][i], sqlite3_column_blob(stmt, i), blob_len);
				break;
			}
			case SQLITE_NULL:
				result->rows[result->row_count - 1][i] = NULL;
				break;
			}
		}
	}

	// Cleanup
	sqlite3_reset(stmt);

	return result;
}

// Function to free the query result
void free_query_result(QueryResult *result)
{
	int i, j;
	for (i = 0; i < result->column_count; i++)
	{
		free(result->column_names[i]);
	}
	free(result->column_names);
	free(result->column_types);

	for (i = 0; i < result->row_count; i++)
	{
		for (j = 0; j < result->column_count; j++)
		{
			if (result->column_types[j] == SQLITE_TEXT || result->column_types[j] == SQLITE_BLOB)
			{
				free(result->rows[i][j]);
			}
			if (result->column_types[j] == SQLITE_INTEGER || result->column_types[j] == SQLITE_FLOAT)
			{
				free(result->rows[i][j]);
			}
		}
		free(result->rows[i]);
	}
	free(result->rows);
	free(result);
}
