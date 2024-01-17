#include "../sqlite3/sqlite3.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct LBDBFile LBDBFile;
struct LBDBFile
{
	sqlite3_file base; /* Base class. Must be first. */
	int main;
	int *pFile;
};

int newVfs();

const extern sqlite3_io_methods lbdb_io_methods;
