#include "statement.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int statement_exec_loop(sqlite3_stmt *stmt, int **column_types, Row **rows, int *row_count)
{
	if (!stmt || !rows || !row_count)
	{
		return SQLITE_MISUSE;
	}

	int rc;
	int row_capacity = 10;
	int col_count = sqlite3_column_count(stmt);

	*column_types = (int *)malloc(col_count * sizeof(int));
	*row_count = 0;
	*rows = (Row *)malloc(row_capacity * sizeof(Row));

	if (!*rows)
		return SQLITE_NOMEM;

	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		if (*row_count == 0)
		{
			for (int i = 0; i < col_count; i++)
			{
				(*column_types)[i] = sqlite3_column_type(stmt, i);
			}
		}

		if (*row_count >= row_capacity)
		{
			row_capacity *= 2;
			Row *temp = (Row *)realloc(*rows, row_capacity * sizeof(Row));
			if (!temp)
				return SQLITE_NOMEM;
			*rows = temp;
		}

		(*rows)[*row_count].columnCount = col_count;
		(*rows)[*row_count].columns = (Column *)malloc(col_count * sizeof(Column));

		for (int i = 0; i < col_count; i++)
		{
			Column *col = &((*rows)[*row_count].columns[i]);
			col->columnType = sqlite3_column_type(stmt, i);

			switch (col->columnType)
			{
			case SQLITE_INTEGER:
				col->length = sizeof(sqlite3_int64);
				col->data = malloc(col->length);
				*(sqlite3_int64 *)col->data = sqlite3_column_int64(stmt, i);
				break;

			case SQLITE_FLOAT:
				col->length = sizeof(double);
				col->data = malloc(col->length);
				*(double *)col->data = sqlite3_column_double(stmt, i);
				break;

			case SQLITE_TEXT:
				col->length = sqlite3_column_bytes(stmt, i);
				col->data = malloc(col->length);
				memcpy(col->data, sqlite3_column_text(stmt, i), col->length);
				break;

			case SQLITE_BLOB:
				col->length = sqlite3_column_bytes(stmt, i);
				col->data = malloc(col->length);
				memcpy(col->data, sqlite3_column_blob(stmt, i), col->length);
				break;

			case SQLITE_NULL:
				col->length = 0;
				col->data = NULL;
				break;
			}
		}
		(*row_count)++;
	}

	return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}
