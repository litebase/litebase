#ifndef LITEBASE_DATA_RANGE_H
#define LITEBASE_DATA_RANGE_H

#include "../sqlite3/sqlite3.h"
#include "./log.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 DataRange represents a range of SQLite database pages that are grouped into.
 a single file.
 */
typedef struct
{
	int file;
	int number;
	char *path;
	int pageSize;
} DataRange;

int pageRange(int pageNumber);
int pageRangeOffset(int pageNumber, int pageSize);

DataRange *NewDataRange(const char *path, int number, int pageSize);

int DataRangeClose(DataRange *dr);
int DataRangeReadAt(DataRange *dr, void *buffer, int iAmt, int pageNumber, int *pReadBytes);
int DataRangeWriteAt(DataRange *dr, const void *buffer, int pageNumber);
int DataRangeRemove(DataRange *dr);
int DataRangeSize(DataRange *dr, int *size);
int DataRangeTruncate(DataRange *dr, int offset);

#endif
