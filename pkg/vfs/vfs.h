#ifndef LITEBASE_VFS_H
#define LITEBASE_VFS_H

#include "../sqlite3/sqlite3.h"

#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef uint16_t u16;
typedef int (*write_hook)(void *, int, sqlite3_int64, const void *);

/* An instance of the VFS */
typedef struct LitebaseVFS
{
	sqlite3_vfs base;  /* VFS methods */
	sqlite3_vfs *pVfs; /* Parent VFS */
	int dataRangesSize;
	void *goVfsPointer;
	int hasPageOne;
	int pageSize;
	void *pShm;
	size_t shmSize;
	char *vfsId;
	write_hook writeHook;
} LitebaseVFS;

typedef struct LitebaseShmRegion
{
	int id; /* Region ID */
	void *pData;
} LitebaseShmRegion;

typedef struct LitebaseShm
{
	sqlite3_mutex *mutex;		  /* Mutex to access this object */
	int nLock;					  /* Number of outstanding locks */
	int *aLock;					  /* Array of outstanding locks */
	u16 exclMask;				  /* Mask of exclusive locks */
	u16 sharedMask;				  /* Mask of shared locks */
	LitebaseShmRegion **pRegions; /* Array of shared memory regions */
	int nRegion;				  /* Number of shared memory regions */
} LitebaseShm;

typedef struct LitebaseVFSFile
{
	sqlite3_file base;	 /* Base class. Must be first. */
	sqlite3_file *pReal; /* Pointer to the real underlying file */

	int isJournal;
	const char *pName;
	char *pVfsId;
	LitebaseShm *pShm;
} LitebaseVFSFile;

int newVfs(char *vfsId, int pageSize);

void unregisterVfs(char *vfsId);

int litebase_is_journal_file(sqlite3_file *pFile);

int litebase_get_shm(sqlite3_file *pFile, void *pShm);

int litebase_write_shm(sqlite3_file *pFile, void *header, int headerSize);

const extern sqlite3_io_methods x_io_methods;

#endif
