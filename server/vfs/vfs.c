#include "./vfs.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int goXOpen(sqlite3_vfs *pVfs, const char *name, sqlite3_file *pFile, int flags, int *outFlags);
extern int goXDelete(sqlite3_vfs *pVfs, const char *name, int syncDir);
extern int goXAccess(sqlite3_vfs *pVfs, const char *name, int flags, int *outRes);
extern int goXFullPathname(sqlite3_vfs *pVfs, const char *name, int nOut, char *out);
extern int goXSleep(sqlite3_vfs *pVfs, int microseconds);

extern int goXClose(sqlite3_file *file);
extern int goXRead(sqlite3_file *file, void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXTruncate(sqlite3_file *file, sqlite3_int64 size);
extern int goXFileSize(sqlite3_file *file, sqlite3_int64 *pSize);
extern int goXLock(sqlite3_file *file, int eLock);
extern int goXUnlock(sqlite3_file *file, int eLock);
extern int goXCheckReservedLock(sqlite3_file *file, int *pResOut);

extern int goXShmMap(sqlite3_file *file, int iPg, int pgsz, int bExtend, void volatile **pp);
extern int goXShmLock(sqlite3_file *file, int offset, int n, int flags);
extern void goXShmBarrier(sqlite3_file *file);
extern int goXShmUnmap(sqlite3_file *file, int deleteFlag);

/*
** Method declarations for LitebaseDBFile.
*/
static int xClose(sqlite3_file *);
static int xRead(sqlite3_file *, void *, int iAmt, sqlite3_int64 iOfst);
static int xWrite(sqlite3_file *, const void *, int iAmt, sqlite3_int64);
static int xTruncate(sqlite3_file *, sqlite3_int64 size);
static int xSync(sqlite3_file *, int flags);
static int xFileSize(sqlite3_file *, sqlite3_int64 *pSize);
static int xLock(sqlite3_file *, int);
static int xUnlock(sqlite3_file *, int);
static int xCheckReservedLock(sqlite3_file *, int *);
static int xFileControl(sqlite3_file *, int op, void *pArg);
static int xSectorSize(sqlite3_file *);
static int xDeviceCharacteristics(sqlite3_file *);
static int xShmLock(sqlite3_file *, int, int, int);
static int xShmMap(sqlite3_file *, int, int, int, void volatile **);
static void xShmBarrier(sqlite3_file *);
static int xShmUnmap(sqlite3_file *, int);
static int xFetch(sqlite3_file *, sqlite3_int64 iOfst, int iAmt, void **pp);
static int xUnfetch(sqlite3_file *, sqlite3_int64 iOfst, void *p);

/* Access to a lower-level VFS that (might) implement dynamic loading,
** access to randomness, etc.
*/
#define ORIGVFS(p) (((LitebaseVFS *)(p))->pVfs)
#define ORIGFILE(p) ((sqlite3_file *)(((LitebaseVFSFile *)(p)) + 1))

int xClose(sqlite3_file *pFile)
{
  // printf("C - xClose\n");
  return goXClose(pFile);
}

int xRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  // printf("C - xRead\n");
  int rc;
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // If the page is not in the cache, read it from the file
  rc = goXRead(pFile, zBuf, iAmt, iOfst);

  if (rc != SQLITE_OK)
  {
    return rc;
  }

  return SQLITE_OK;
}

int xWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  // printf("C - xWrite\n");

  return goXWrite(pFile, zBuf, iAmt, iOfst);
}

// truncate
int xTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
  // printf("C - xTruncate\n");
  return goXTruncate(pFile, size);
}

int xSync(sqlite3_file *pFile, int flags)
{
  // printf("C - xSync\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return p->pReal->pMethods->xSync(p->pReal, flags);
  // return 0;
}

int xFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;
  // printf("C - xFileSize %s\n", p->pVfsId);
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
  // return goXFileSize(pFile, pSize);
}

int xLock(sqlite3_file *pFile, int eLock)
{
  // return SQLITE_OK;
  printf("C - xLock\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // Check if the lock is already held
  // if (p->pReal->pMethods->xCheckReservedLock(pFile, &eLock) == SQLITE_LOCKED)
  // {
  //   // The lock is already held, return SQLITE_OK
  //   return SQLITE_OK;
  // }

  return p->pReal->pMethods->xLock(p->pReal, eLock);
}

int xUnlock(sqlite3_file *pFile, int eLock)
{
  // return SQLITE_OK;
  printf("C - xUnlock\n");
  // return goXUnlock(pFile, eLock);

  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

int xCheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
  // printf("C - xCheckReservedLock\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);

  // return goXCheckReservedLock(pFile, pResOut);
}

int xFileControl(sqlite3_file *pFile, int op, void *pArg)
{
  // printf("C - xFileControl\n");

  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // printf("C - xFileControl %d\n", op);

  return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
  // const char *name = ((LitebaseVFSFile *)pFile)->name;

  // switch (op)
  // {
  // case SQLITE_FCNTL_PERSIST_WAL:
  //   // Enable persistent WAL
  //   *(int *)pArg = 1;
  //   return SQLITE_OK;
  // case SQLITE_FCNTL_WAL_BLOCK:
  //   // Enable blocking WAL
  //   return SQLITE_OK;
  // default:
  //   // Other operations are not supported
  //   return SQLITE_NOTFOUND;
  // }
}

int xSectorSize(sqlite3_file *pFile)
{
  // printf("C - xSectorSize\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return p->pReal->pMethods->xSectorSize(p->pReal);

  // return 0;
}

int xDeviceCharacteristics(sqlite3_file *pFile)
{
  // printf("C - xDeviceCharacteristics\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
  // return SQLITE_IOCAP_ATOMIC;
}

int xShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp)
{
  // printf("C - xShmMap\n");
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;
  if (p->pReal->pMethods->xShmMap)
  {
    printf("p->pReal->pMethods->xShmMap, iPg: %d, pgsz %d, bExtend %d\n", iPg, pgsz, bExtend);
  }

  int rc = p->pReal->pMethods->xShmMap(pFile, iPg, pgsz, bExtend, pp);

  // printf("rc %d\n", rc);
  if (rc != SQLITE_OK)
  {
    return rc;
  }

  return SQLITE_OK;

  // return ORIGFILE(pFile)
  //     ->pMethods->xShmMap(ORIGFILE(pFile), iPg, pgsz, bExtend, pp);
  // // // return sqlite3OsShmMap(pFile, iPg, pgsz, x, pp);

  // // allocate the space for the shared memory if it is not already allocated
  // *pp = malloc(pgsz);

  // // return ORIGFILE(pFile)->pMethods->xShmMap(ORIGFILE(pFile), iPg, pgsz, x, pp);
  // // return ORIGFILE(pFile)->pMethods->xShmMap(ORIGFILE(pFile), iPg, pgsz, x, pp);
  // return goXShmMap(pFile, iPg, pgsz, bExtend, pp);
}

int xShmLock(sqlite3_file *pFile, int offset, int n, int flags)
{
  // printf("C - xShmLock\n");
  return ORIGFILE(pFile)->pMethods->xShmLock(ORIGFILE(pFile), offset, n, flags);
  // return ORIGFILE(pFile)->pMethods->xShmLock(ORIGFILE(pFile), offset, n, flags);
  // return goXShmLock(pFile, offset, n, flags);
}

void xShmBarrier(sqlite3_file *pFile)
{
  // printf("C - xShmBarrier\n");

  // ORIGFILE(pFile)->pMethods->xShmBarrier(ORIGFILE(pFile));

  // goXShmBarrier(pFile);
  // return ORIGFILE(pFile)->pMethods->xShmBarrier(ORIGFILE(pFile));
}

int xShmUnmap(sqlite3_file *pFile, int deleteFlag)
{
  // printf("C - xShmUnmap\n");

  return ORIGFILE(pFile)->pMethods->xShmUnmap(ORIGFILE(pFile), deleteFlag);
  // return goXShmUnmap(pFile, deleteFlag);
}

static int xFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp)
{
  // printf("C - xFetch\n");
  return SQLITE_OK;
}

static int xUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *p)
{
  // printf("C - xUnfetch\n");
  return SQLITE_OK;
}

int xOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags)
{
  char *vfsId = pVfs->pAppData;
  char *fileVfsId = malloc(strlen(vfsId) + 1);

  strcpy(fileVfsId, vfsId);

  int rc;

  // If this is the main file
  // if (flags & SQLITE_OPEN_MAIN_DB)
  // {
  rc = goXOpen(pVfs, zName, pFile, flags, pOutFlags);
  // }

  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  p->pReal = (sqlite3_file *)&p[1];
  rc = ORIGVFS(pVfs)->xOpen(ORIGVFS(pVfs), zName, p->pReal, flags, pOutFlags);

  p->pName = zName;
  p->pVfsId = fileVfsId;
  pFile->pMethods = &x_io_methods;

  return rc;
}

static int xDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir)
{
  return goXDelete(pVfs, zName, syncDir);
}

static int xAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut)
{
  // printf("C - xAccess\n");
  LitebaseVFS *p = (LitebaseVFS *)pVfs;

  return ORIGVFS(pVfs)->xAccess(p->pVfs, zName, flags, pResOut);
  // return goXAccess(pVfs, zName, flags, pResOut);
}

static int xFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut)
{
  // printf("C - xFullPathname %s\n", zName);

  LitebaseVFS *p = (LitebaseVFS *)pVfs;

  return ORIGVFS(pVfs)->xFullPathname(p->pVfs, zName, nOut, zOut);
}

static void *xDlOpen(sqlite3_vfs *pVfs, const char *zFilename)
{
  return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zFilename);
}

static void xDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
  ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}

static void (*xDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void)
{
  return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}

static void xDlClose(sqlite3_vfs *pVfs, void *pHandle)
{
  ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}

int xSleep(sqlite3_vfs *pVfs, int microseconds)
{
  // printf("xSleep\n");
  return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), microseconds);
}

int xRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte)
{
  // printf("xRandomness\n");
  return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zByte);
}

int xCurrentTime(sqlite3_vfs *pVfs, double *pTime)
{
  // printf("xCurrentTime\n");
  return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTime);
}

int xGetLastError(sqlite3_vfs *pVfs, int a, char *b)
{
  return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}

int xCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTime)
{
  // printf("xCurrentTimeInt64\n");
  return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), pTime);
}

static LitebaseVFS litebase_vfs = {
    {
        2,                 /* iVersion */
        0,                 /* szOsFile */
        1024,              /* mxPathname */
        0,                 /* pNext */
        "litebase",        /* zName */
        0,                 /* pAppData */
        xOpen,             /* xOpen */
        xDelete,           /* xDelete */
        xAccess,           /* xAccess */
        xFullPathname,     /* xFullPathname */
        xDlOpen,           /* xDlOpen */
        xDlError,          /* xDlError */
        xDlSym,            /* xDlSym */
        xDlClose,          /* xDlClose */
        xRandomness,       /* xRandomness */
        xSleep,            /* xSleep */
        xCurrentTime,      /* xCurrentTime */
        xGetLastError,     /* xGetLastError */
        xCurrentTimeInt64, /* xCurrentTimeInt64 */
    },
    0,
};

const sqlite3_io_methods x_io_methods = {
    3,                      /* iVersion */
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
    xShmMap,                /* xShmMap */
    xShmLock,               /* xShmLock */
    xShmBarrier,            /* xShmBarrier */
    xShmUnmap               /* xShmUnmap */
};

int register_litebase_vfs(char *vfsId)
{
  // TODO: Free this memory when the VFS is unregistered
  char *pVfsId = malloc(strlen(vfsId) + 1);
  strcpy(pVfsId, vfsId);

  sqlite3_vfs *pOrig = sqlite3_vfs_find(0);
  litebase_vfs.pVfs = pOrig;
  litebase_vfs.vfsId = pVfsId;

  if (litebase_vfs.pVfs == 0)
  {
    return SQLITE_ERROR;
  }

  litebase_vfs.base.szOsFile = sizeof(LitebaseVFSFile) + litebase_vfs.pVfs->szOsFile;
  litebase_vfs.base.zName = pVfsId;
  litebase_vfs.base.pAppData = pVfsId;

  return sqlite3_vfs_register(&litebase_vfs.base, 0);
}

void logCallback(void *pArg, int iErrCode, const char *zMsg)
{
  fprintf(stderr, "SQLITE_LOG: (%d) %s\n", iErrCode, zMsg);
}

int newVfs(char *vfsId)
{
  sqlite3_config(SQLITE_CONFIG_LOG, logCallback, NULL);

  return register_litebase_vfs(vfsId);
}
