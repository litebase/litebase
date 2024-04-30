#include "../sqlite3/sqlite3.h"
#include <stdio.h>
#include <stdlib.h>

/* An instance of the VFS */
typedef struct LitebaseVFS
{
	sqlite3_vfs base;  /* VFS methods */
	sqlite3_vfs *pVfs; /* Parent VFS */
	char *vfsId;
} LitebaseVFS;

typedef struct LitebaseVFSFile
{
	sqlite3_file base;	 /* Base class. Must be first. */
	sqlite3_file *pReal; /* Pointer to the real underlying file */
	const char *pName;
	const char *pVfsId;
} LitebaseVFSFile;

int newVfs(char *vfsId);

const extern sqlite3_io_methods x_io_methods;
