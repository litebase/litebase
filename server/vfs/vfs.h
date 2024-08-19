#include "../sqlite3/sqlite3.h"
#include "./data_range.h"

typedef int (*write_hook)(void *, int, sqlite3_int64, const void *);

/* An instance of the VFS */
typedef struct LitebaseVFS
{
	sqlite3_vfs base;  /* VFS methods */
	sqlite3_vfs *pVfs; /* Parent VFS */
	char *dataPath;
	DataRange **dataRanges;
	int dataRangesSize;
	void *goVfsPointer;
	int hasPageOne;
	int pageSize;
	char *vfsId;
	write_hook writeHook;
} LitebaseVFS;

typedef struct LitebaseVFSFile
{
	sqlite3_file base;	 /* Base class. Must be first. */
	sqlite3_file *pReal; /* Pointer to the real underlying file */

	int isJournal;
	const char *pName;
	char *pVfsId;
} LitebaseVFSFile;

int newVfs(char *vfsId, char *dataPath, int pageSize);

void unregisterVfs(char *vfsId);

int litebase_is_journal_file(sqlite3_file *pFile);

int litebase_vfs_write_hook(char *vfsId, int (*)(void *, int, sqlite3_int64, const void *), void *);

const extern sqlite3_io_methods x_io_methods;
