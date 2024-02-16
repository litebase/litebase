#include "../sqlite3/sqlite3.h"
#include "./litebase_vfs_page_cache.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct LBDBFile LBDBFile;
struct LBDBFile
{
	sqlite3_file base; /* Base class. Must be first. */
	int main;
	int *pFile;
	LitebaseVFSCache *cache;
};

int newVfs();

const extern sqlite3_io_methods x_io_methods;
