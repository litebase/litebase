#include "./vfs.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

extern int goXOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file, int flags, int *outFlags, char *fileID);
extern int goXDelete(sqlite3_vfs *vfs, const char *name, int syncDir);
extern int goXAccess(sqlite3_vfs *vfs, const char *name, int flags, int *outRes);
extern int goXSleep(sqlite3_vfs *, int microseconds);

extern int goXClose(sqlite3_file *file);
extern int goXRead(sqlite3_file *file, void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXFileSize(sqlite3_file *file, sqlite3_int64 *pSize);
extern int goXLock(sqlite3_file *file, int eLock);
extern int goXUnlock(sqlite3_file *file, int eLock);
extern int goXCheckReservedLock(sqlite3_file *file, int *pResOut);

static struct
{
  /* The pOrigVfs is the real, original underlying VFS implementation.
   ** Most operations pass-through to the real VFS.  This value is read-only
   ** during operation.  It is only modified at start-time and thus does not
   ** require a mutex.
   */
  sqlite3_vfs *pOrigVfs;

  /* The vfs is the VFS structure used by this shim.  It is initialized
  ** at start-time and thus does not require a mutex
  */
  sqlite3_vfs vfs;

  /* The sIoMethods defines the methods used by sqlite3_file objects
  ** associated with this shim.  It is initialized at start-time and does
  ** not require a mutex.
  **
  ** When the underlying VFS is called to open a file, it might return
  ** either a version 1 or a version 2 sqlite3_file object.  This shim
  ** has to create a wrapper sqlite3_file of the same version.  Hence
  ** there are two I/O method structures, one for version 1 and the other
  ** for version 2.
  */
  sqlite3_io_methods sIoMethodsV1;
  sqlite3_io_methods sIoMethodsV2;

  /* True when this shim as been initialized.
   */
  int isInitialized;

  /* For run-time access any of the other global data structures in this
  ** shim, the following mutex must be held.
  */
  sqlite3_mutex *pMutex;

  char id;

  P1Cache *cache;
} x;

static sqlite3_vfs *xRootVFS() { return x.pOrigVfs; }

/* Translate an sqlite3_file* that is really a LBDBFile* into
** the sqlite3_file* for the underlying original VFS.
*/
static sqlite3_file *xFile(sqlite3_file *pFile)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return (sqlite3_file *)&p[1];
}

int xRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  // Get the page from the cache
  if (iAmt != 4096)
  {
    return goXRead(pFile, zBuf, iAmt, iOfst);
  }

  int rc;
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // Calculate the page number
  int pageNumber = iOfst / 4096 + 1;

  int ok = p->p1Cache->Get(p->p1Cache, pageNumber, zBuf);

  // If the page is not in the cache, read it from the file
  if (ok != 0)
  {
    // If the page is not in the cache, read it from the file
    rc = goXRead(pFile, zBuf, iAmt, iOfst);

    if (rc != SQLITE_OK)
    {
      return rc;
    }

    // Then put the page in the cache if zBuf is 4096 bytes
    p->p1Cache->Put(p->p1Cache, pageNumber, zBuf);
  }

  return SQLITE_OK;
}

int xWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // Calculate the page number
  int pageNumber = iOfst / 4096 + 1;

  // If the page is in the cache, delete it
  p->p1Cache->Delete(p->p1Cache, pageNumber);

  int rc = goXWrite(pFile, zBuf, iAmt, iOfst);

  return rc;
}

// truncate
int xTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
  // TODO: implement
  return 0;
}

int xSync(sqlite3_file *pFile, int flags) { return 0; }

int xFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
  return goXFileSize(pFile, pSize);
}

int xLock(sqlite3_file *pFile, int eLock) { return goXLock(pFile, eLock); }

int xUnlock(sqlite3_file *pFile, int eLock) { return goXUnlock(pFile, eLock); }

int xCheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
  return goXCheckReservedLock(pFile, pResOut);
}

int xFileControl(sqlite3_file *pFile, int op, void *pArg)
{
  return SQLITE_NOTFOUND;

  return 0;
}

int xSectorSize(sqlite3_file *pFile) { return 0; }

int xDeviceCharacteristics(sqlite3_file *pFile) { return 0; }

int xShmMap(sqlite3_file *pFile, int iPg, int pgsz, int x, void volatile **pp)
{
  // printf("xShmMap:\n");
  return xFile(pFile)->pMethods->xShmMap(xFile(pFile), iPg, pgsz, x, pp);
}

int xShmLock(sqlite3_file *pFile, int offset, int n, int flags)
{
  // printf("xShmLock:\n");

  return xFile(pFile)->pMethods->xShmLock(xFile(pFile), offset, n, flags);
}

void xShmBarrier(sqlite3_file *pFile)
{
  // printf("xShmBarrier:\n");
  xFile(pFile)->pMethods->xShmBarrier(xFile(pFile));
}

int xShmUnmap(sqlite3_file *pFile, int deleteFlag)
{
  // printf("xShmUnmap:\n");

  return xFile(pFile)->pMethods->xShmUnmap(xFile(pFile), deleteFlag);
}

int xOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags)
{
  // Return the root VFS xOpen method if this is not a main database
  if ((flags & SQLITE_OPEN_MAIN_DB) == 0)
  {
    return xRootVFS()->xOpen(pVfs, zName, pFile, flags, pOutFlags);
  }

  char *fileID;
  fileID = (char *)malloc(sizeof(char) * 64);

  int rc = goXOpen(pVfs, zName, pFile, flags, pOutFlags, fileID);

  pFile->pMethods = &x_io_methods;

  ((LitebaseVFSFile *)pFile)->id = fileID;

  // Set the cache
  // 3125000 * 4096 bytes = 128MB
  ((LitebaseVFSFile *)pFile)->p1Cache = createCache(fileID, 3125000);

  return rc;
}

int xDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir)
{
  // printf("xDelete: %s\n", zName);
  if (pVfs != &x.vfs)
  {
    return xRootVFS()->xDelete(pVfs, zName, syncDir);
  }

  return goXDelete(pVfs, zName, syncDir);
}

int xAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut)
{
  if ((flags & SQLITE_OPEN_MAIN_DB) == 0)
  {
    return xRootVFS()->xAccess(pVfs, zName, flags, pResOut);
  }

  // printf("Access:\n");

  return goXAccess(pVfs, zName, flags, pResOut);
}

int xSleep(sqlite3_vfs *vfs, int microseconds)
{
  return goXSleep(vfs, microseconds);
}

int xClose(sqlite3_file *pFile) { return goXClose(pFile); }

int register_litebase_vfs()
{
  sqlite3_vfs *pOrigVfs;

  if (x.isInitialized)
  {
    return SQLITE_MISUSE;
  }

  pOrigVfs = sqlite3_vfs_find(0);

  if (pOrigVfs == 0)
  {
    return SQLITE_ERROR;
  }

  assert(pOrigVfs != &x.vfs);

  x.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);

  if (!x.pMutex)
  {
    return SQLITE_NOMEM;
  }

  x.isInitialized = 1;
  x.pOrigVfs = pOrigVfs;
  x.vfs = *pOrigVfs;
  x.vfs.zName = "litebase";
  x.vfs.xOpen = xOpen;
  x.vfs.xDelete = xDelete;
  x.vfs.xAccess = xAccess;
  x.vfs.xSleep = xSleep;
  x.vfs.szOsFile += sizeof(LitebaseVFSFile);
  x.sIoMethodsV1.iVersion = 2;
  // x.sIoMethodsV1.xClose = xClose;
  // x.sIoMethodsV1.xRead = xRead;
  // x.sIoMethodsV1.xWrite = xWrite;
  // x.sIoMethodsV1.xTruncate = xTruncate;
  // x.sIoMethodsV1.xSync = xSync;
  // x.sIoMethodsV1.xFileSize = xFileSize;
  // x.sIoMethodsV1.xLock = xLock;
  // x.sIoMethodsV1.xUnlock = xUnlock;
  // x.sIoMethodsV1.xCheckReservedLock = xCheckReservedLock;
  // x.sIoMethodsV1.xFileControl = xFileControl;
  // x.sIoMethodsV1.xSectorSize = xSectorSize;
  // x.sIoMethodsV1.xDeviceCharacteristics = xDeviceCharacteristics;
  // x.sIoMethodsV2 = x.sIoMethodsV1;
  // x.sIoMethodsV2.iVersion = 2;
  // x.sIoMethodsV2.xShmMap = xShmMap;
  // x.sIoMethodsV2.xShmLock = xShmLock;
  // x.sIoMethodsV2.xShmBarrier = xShmBarrier;
  // x.sIoMethodsV2.xShmUnmap = xShmUnmap;

  sqlite3_vfs_register(&x.vfs, 1);

  return SQLITE_OK;
}

void errorLogCallback(void *pArg, int iErrCode, const char *zMsg)
{
  fprintf(stderr, "(%d) %s\n", iErrCode, zMsg);
}

int newVfs()
{
  sqlite3_config(SQLITE_CONFIG_LOG, errorLogCallback, NULL);

  return register_litebase_vfs();
}

const sqlite3_io_methods x_io_methods = {
    1,                      /* iVersion */
    xClose,                 /* xClose */
    xRead,                  /* xRead */
    xWrite,                 /* xWrite */
    xTruncate,              /* xTruncate */
    xSync,                  /* xSync */
    xFileSize,              /* xFileSize */
    xLock,                  /* xLock */
    xUnlock,                /* xUnlock */
    xCheckReservedLock,     /* xCheckReservedLock */
    xFileControl,           /* xFileControl */
    xSectorSize,            /* xSectorSize */
    xDeviceCharacteristics, /* xDeviceCharacteristics */
};
