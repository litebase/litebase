#include "../sqlite3/sqlite3.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct
{
	int file;
	int number;
	char *path;
	int pageSize;
} DataRange;

int pageRange(int pageNumber);

DataRange *NewDataRange(const char *path, int number, int pageSize);

void CloseDataRange(DataRange *dr);
int DataRangeReadAt(DataRange *dr, void *buffer, int iAmt, int pageNumber, int *pReadBytes);
int DataRangeWriteAt(DataRange *dr, const void *buffer, int pageNumber);
