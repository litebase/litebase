#include "../sqlite3/sqlite3.h"
#include "./p1_cache.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct LitebaseVFSFile LitebaseVFSFile;
struct LitebaseVFSFile
{
	sqlite3_file base; /* Base class. Must be first. */
	char *id;
	int main;
	int *pFile;
	P1Cache *p1Cache;
};

int newVfs();

const extern sqlite3_io_methods x_io_methods;
