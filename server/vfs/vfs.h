#include "../sqlite3/sqlite3.h"

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
	char *pVfsId;
} LitebaseVFSFile;

int newVfs(char *vfsId);

void unregisterVfs(char *vfsId);

int litebase_is_journal_file(sqlite3_file *pFile);

const extern sqlite3_io_methods x_io_methods;
