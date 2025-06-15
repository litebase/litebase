#include "sqlite3.h"

#ifndef STATEMENT_H
#define STATEMENT_H

typedef struct
{
	int columnType;
	int length;
	void *data;
} Column;

typedef struct
{
	int columnCount;
	Column *columns;
} Row;

int statement_exec_loop(sqlite3_stmt *stmt, int **column_types, Row **rows, int *row_count);

#endif
