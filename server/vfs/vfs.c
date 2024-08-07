#include "./vfs.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LOG_ENABLED 0

void vfs_log(const char *format, ...)
{
  if (!LOG_ENABLED)
  {
    return;
  }

  // Get the current time
  time_t now = time(NULL);
  struct tm *local_time = localtime(&now);

  // Determine the log level name
  const char *level_name;
  // Print the log message
  printf("[%02d:%02d:%02d] [VFS LOG] ", local_time->tm_hour, local_time->tm_min, local_time->tm_sec);

  // Use the variable argument list to print the message
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);

  printf("\n");
}

int vfs_log_start()
{
  if (!LOG_ENABLED)
  {
    return 0;
  }

  // Get the current time
  int starttime;
  struct timespec start;

  clock_gettime(CLOCK_MONOTONIC, &start);

  return start.tv_sec * 1000000000L + start.tv_nsec;
}

void vfs_log_end(int starttime, const char *description, ...)
{
  if (!LOG_ENABLED)
  {
    return;
  }

  int endtime;

  // Get the current time
  struct timespec end;

  clock_gettime(CLOCK_MONOTONIC, &end);

  endtime = end.tv_sec * 1000000000L + end.tv_nsec;

  // Print the log message
  printf("[%s] - took %d nanoseconds\n", description, endtime - starttime);

  printf("\n");
}

extern int
goXOpen(sqlite3_vfs *pVfs, const char *name, sqlite3_file *pFile, int flags, int *outFlags);
extern int goXDelete(sqlite3_vfs *pVfs, const char *name, int syncDir);
extern int goXAccess(sqlite3_vfs *pVfs, const char *name, int flags, int *outRes);

extern int goXClose(sqlite3_file *file);
extern int goXRead(sqlite3_file *file, void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXTruncate(sqlite3_file *file, sqlite3_int64 size);
extern int goXFileSize(sqlite3_file *file, sqlite3_int64 *pSize);

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
#define ORIGFILE(p) ((sqlite3_file *)(((LitebaseVFSFile *)(p))->pReal))

static int vfsInstancesSize = 0;
static LitebaseVFS **vfsInstances = NULL;

int xClose(sqlite3_file *pFile)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // Free the memory allocated for the VFS ID
  free(p->pVfsId);

  if (litebase_is_journal_file(pFile))
  {
    return ORIGFILE(pFile)->pMethods->xClose(ORIGFILE(pFile));
  }

  return goXClose(pFile);
}

int xRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  int rc;

  if (litebase_is_journal_file(pFile))
  {
    rc = ORIGFILE(pFile)->pMethods->xRead(ORIGFILE(pFile), zBuf, iAmt, iOfst);
  }
  else
  {
    rc = goXRead(pFile, zBuf, iAmt, iOfst);
  }

  return rc;
}

int xWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  int rc;

  if (litebase_is_journal_file(pFile))
  {
    rc = ORIGFILE(pFile)->pMethods->xWrite(ORIGFILE(pFile), zBuf, iAmt, iOfst);
  }
  else
  {
    rc = goXWrite(pFile, zBuf, iAmt, iOfst);
  }

  return rc;
}

// truncate
int xTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
  if (litebase_is_journal_file(pFile))
  {
    return ORIGFILE(pFile)->pMethods->xTruncate(ORIGFILE(pFile), size);
  }

  return goXTruncate(pFile, size);
}

int xSync(sqlite3_file *pFile, int flags)
{
  return ORIGFILE(pFile)->pMethods->xSync(ORIGFILE(pFile), flags);
}

int xFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
  int rc;

  if (litebase_is_journal_file(pFile))
  {
    rc = ORIGFILE(pFile)->pMethods->xFileSize(ORIGFILE(pFile), pSize);
  }
  else
  {
    rc = goXFileSize(pFile, pSize);
  }

  return rc;
}

int xLock(sqlite3_file *pFile, int eLock)
{
  return ORIGFILE(pFile)->pMethods->xLock(ORIGFILE(pFile), eLock);
}

int xUnlock(sqlite3_file *pFile, int eLock)
{
  return ORIGFILE(pFile)->pMethods->xUnlock(ORIGFILE(pFile), eLock);
}

int xCheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
  return ORIGFILE(pFile)->pMethods->xCheckReservedLock(ORIGFILE(pFile), pResOut);
}

int xFileControl(sqlite3_file *pFile, int op, void *pArg)
{
  return ORIGFILE(pFile)->pMethods->xFileControl(ORIGFILE(pFile), op, pArg);
}

int xSectorSize(sqlite3_file *pFile)
{
  return ORIGFILE(pFile)->pMethods->xSectorSize(ORIGFILE(pFile));
}

int xDeviceCharacteristics(sqlite3_file *pFile)
{
  return ORIGFILE(pFile)->pMethods->xDeviceCharacteristics(ORIGFILE(pFile));
}

int xShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp)
{
  return ORIGFILE(pFile)->pMethods->xShmMap(ORIGFILE(pFile), iPg, pgsz, bExtend, pp);
}

int xShmLock(sqlite3_file *pFile, int offset, int n, int flags)
{
  int rc;

  // TODO: Test if we really need a shared memory lock or if this should be a
  // no-op since our primary will be the only writer and writes are always
  // synchronized from clients.

  // rc = ORIGFILE(pFile)->pMethods->xShmLock(ORIGFILE(pFile), offset, n, flags);
  
  rc = SQLITE_OK;

  return rc;
}

void xShmBarrier(sqlite3_file *pFile)
{
  int starttime;

  ORIGFILE(pFile)->pMethods->xShmBarrier(ORIGFILE(pFile));
}

int xShmUnmap(sqlite3_file *pFile, int deleteFlag)
{
  return ORIGFILE(pFile)->pMethods->xShmUnmap(ORIGFILE(pFile), deleteFlag);
}

static int xFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp)
{
  return ORIGFILE(pFile)->pMethods->xFetch(ORIGFILE(pFile), iOfst, iAmt, pp);
}

static int xUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *p)
{
  return ORIGFILE(pFile)->pMethods->xUnfetch(ORIGFILE(pFile), iOfst, p);
}

int xOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags)
{
  char *vfsId = pVfs->pAppData;
  char *fileVfsId = malloc(strlen(vfsId) + 1);

  strcpy(fileVfsId, vfsId);

  int rc;

  rc = goXOpen(pVfs, zName, pFile, flags, pOutFlags);

  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  p->pReal = (sqlite3_file *)&p[1];

  rc = ORIGVFS(pVfs)->xOpen(ORIGVFS(pVfs), zName, ORIGFILE(pFile), flags, pOutFlags);

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
  // If the file is a WAL file, call the original xAccess method
  if (strstr(zName, "-wal") != NULL || strstr(zName, "-journal") != NULL)
  {
    return ORIGVFS(pVfs)->xAccess(ORIGVFS(pVfs), zName, flags, pResOut);
  }

  return goXAccess(pVfs, zName, flags, pResOut);
}

static int xFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut)
{
  return ORIGVFS(pVfs)->xFullPathname(ORIGVFS(pVfs), zName, nOut, zOut);
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
  return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), microseconds);
}

int xRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte)
{
  return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zByte);
}

int xCurrentTime(sqlite3_vfs *pVfs, double *pTime)
{
  return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTime);
}

int xGetLastError(sqlite3_vfs *pVfs, int a, char *b)
{
  return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}

int xCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTime)
{
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
    xShmUnmap,              /* xShmUnmap */
    xFetch,                 /* xFetch */
    xUnfetch                /* xUnfetch */
};

int register_litebase_vfs(char *vfsId)
{
  vfs_log("Registering Litebase VFS");

  char *pVfsId = malloc(strlen(vfsId) + 1);
  strcpy(pVfsId, vfsId);

  sqlite3_vfs *pOrig = sqlite3_vfs_find(0);
  litebase_vfs.base.zName = pVfsId;
  litebase_vfs.pVfs = pOrig;
  litebase_vfs.vfsId = pVfsId;

  if (litebase_vfs.pVfs == 0)
  {
    printf("Failed to find the default VFS\n");
    return SQLITE_ERROR;
  }

  litebase_vfs.base.szOsFile = sizeof(LitebaseVFSFile) + litebase_vfs.pVfs->szOsFile;
  litebase_vfs.base.zName = pVfsId;
  litebase_vfs.base.pAppData = pVfsId;

  vfsInstancesSize++;

  // realloc the vfsInstances array
  vfsInstances = realloc(vfsInstances, sizeof(LitebaseVFS *) * vfsInstancesSize);

  // Push the new VFS instance to the list
  LitebaseVFS *vfs = malloc(sizeof(LitebaseVFS));
  memcpy(vfs, &litebase_vfs, sizeof(LitebaseVFS));
  vfsInstances[vfsInstancesSize - 1] = vfs;

  return sqlite3_vfs_register(&vfs->base, 0);
}

void unregisterVfs(char *vfsId)
{
  vfs_log("Unregistering Litebase VFS");

  for (int i = 0; i < vfsInstancesSize; i++)
  {
    if (strcmp(vfsInstances[i]->vfsId, vfsId) == 0)
    {
      sqlite3_vfs *pVfs = sqlite3_vfs_find(vfsId);
      if (pVfs == 0)
      {
        printf("Failed to find the VFS\n");
        return;
      }

      // Free the memory allocated for the VFS ID
      free(pVfs->pAppData);

      int rc = sqlite3_vfs_unregister(pVfs);
      if (rc != SQLITE_OK)
      {
        printf("Failed to unregister the VFS: %d\n", rc);
        return;
      }

      // Free the memory allocated for vfsInstances[i]
      free(vfsInstances[i]);

      // Resize the vfsInstances array
      for (int j = i; j < vfsInstancesSize - 1; j++)
      {
        vfsInstances[j] = vfsInstances[j + 1];
      }

      vfsInstancesSize--;

      // Realloc the vfsInstances array and check for errors
      LitebaseVFS **newVfsInstances = realloc(vfsInstances, sizeof(LitebaseVFS *) * vfsInstancesSize);
      if (newVfsInstances == NULL)
      {
        printf("Failed to realloc vfsInstances\n");
        return;
      }

      vfsInstances = newVfsInstances;
    }
  }
}

int litebase_is_journal_file(sqlite3_file *pFile)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;

  // Check if the file name ends with "-wal"
  size_t len = strlen(p->pName);

  return len >= 4 && (strcmp(p->pName + len - 4, "-wal") == 0 || strcmp(p->pName + len - 8, "-journal") == 0);
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
