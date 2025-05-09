#include "./vfs.h"
#include "./log.h"

extern int goXOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags);

extern int goXClose(sqlite3_file *file);
extern int goXRead(sqlite3_file *file, void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXTruncate(sqlite3_file *file, sqlite3_int64 size);
extern int goXFileSize(sqlite3_file *file, sqlite3_int64 *pSize);
// extern int goXSync(sqlite3_file *file, int flags);

extern int goXWALFileSize(sqlite3_file *file, sqlite3_int64 *pSize);
extern int goXWALRead(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst);
extern int goXWALWrite(sqlite3_file *file, int iAmt, sqlite3_int64 iOfst, const void *buf);
extern int goXWALTruncate(sqlite3_file *file, sqlite3_int64 size);
extern int goXWALSync(sqlite3_file *file, int flags);

extern int goXShmMap(sqlite3_file *file, int iPg, int pgSize, int bExtend, void volatile **pp);
extern int goXShmLock(sqlite3_file *file, int offset, int n, int flags);
extern int goXShmUnmap(sqlite3_file *file, int deleteFlag);
extern void goXShmBarrier(sqlite3_file *file);

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

  int rc = ORIGFILE(pFile)->pMethods->xClose(ORIGFILE(pFile));

  // Free the memory allocated for the VFS ID
  free(p->pVfsId);

  p->pVfsId = NULL;

  return rc;
}

int xRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  if (((LitebaseVFSFile *)pFile)->isJournal)
  {
    return goXWALRead(pFile, zBuf, iAmt, iOfst);
  }

  return goXRead(pFile, zBuf, iAmt, iOfst);
}

int xWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
  if (((LitebaseVFSFile *)pFile)->isJournal)
  {
    return goXWALWrite(pFile, iAmt, iOfst, zBuf);
  }

  return goXWrite(pFile, zBuf, iAmt, iOfst);
}

int xTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
  int rc = SQLITE_OK;

  if (((LitebaseVFSFile *)pFile)->isJournal)
  {
    // return ORIGFILE(pFile)->pMethods->xTruncate(ORIGFILE(pFile), size);

    return goXWALTruncate(pFile, size);
  }

  return goXTruncate(pFile, size);
}

int xSync(sqlite3_file *pFile, int flags)
{

  if (((LitebaseVFSFile *)pFile)->isJournal)
  {
    // vfs_log("WAL- xSync");
    return goXWALSync(pFile, flags);
  }

  // vfs_log("xSync");
  // This is a no-op for the main file.
  return SQLITE_OK;
}

int xFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
  int rc = SQLITE_OK;

  if (((LitebaseVFSFile *)pFile)->isJournal)
  {
    // return ORIGFILE(pFile)->pMethods->xFileSize(ORIGFILE(pFile), pSize);

    return goXWALFileSize(pFile, pSize);
  }

  return goXFileSize(pFile, pSize);
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

int xShmMap(sqlite3_file *pFile, int iPg, int pgSize, int bExtend, void volatile **pp)
{
  // vfs_log("xShmMap");
  int rc = goXShmMap(pFile, iPg, pgSize, bExtend, (void volatile **)pp);
  // int rc = ORIGFILE(pFile)->pMethods->xShmMap(ORIGFILE(pFile), iPg, pgSize, bExtend, (void volatile **)pp);

  return rc;
}

int xShmUnmap(sqlite3_file *pFile, int deleteFlag)
{
  // vfs_log("xShmUnmap");
  int rc = goXShmUnmap(pFile, deleteFlag);
  // int rc = ORIGFILE(pFile)->pMethods->xShmUnmap(ORIGFILE(pFile), deleteFlag);

  return rc;
}

int xShmLock(sqlite3_file *pFile, int offset, int n, int flags)
{
  // vfs_log("xShmLock");
  return goXShmLock(pFile, offset, n, flags);
  // return ORIGFILE(pFile)->pMethods->xShmLock(ORIGFILE(pFile), offset, n, flags);
}

void xShmBarrier(sqlite3_file *pFile)
{
  // vfs_log("xShmBarrier");
  goXShmBarrier(pFile);
  // ORIGFILE(pFile)->pMethods->xShmBarrier(ORIGFILE(pFile));
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
  int rc;
  char *fileVfsId = malloc(strlen(pVfs->zName) + 1);
  strcpy(fileVfsId, pVfs->zName);

  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;
  p->pName = zName;
  p->isJournal = litebase_is_journal_file(pFile);
  p->pVfsId = fileVfsId;
  p->pReal = (sqlite3_file *)&p[1];

  if (p->isJournal == 0)
  {
    rc = goXOpen(pVfs, zName, pFile, flags, pOutFlags);
  }

  rc = ORIGVFS(pVfs)->xOpen(ORIGVFS(pVfs), zName, ORIGFILE(pFile), flags, pOutFlags);

  pFile->pMethods = &x_io_methods;

  return rc;
}

static int xDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir)
{
  return ORIGVFS(pVfs)->xDelete(ORIGVFS(pVfs), zName, syncDir);
}

static int xAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut)
{
  return ORIGVFS(pVfs)->xAccess(ORIGVFS(pVfs), zName, flags, pResOut);
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
        2048,              /* mxPathname */
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

int register_litebase_vfs(char *vfsId, int pageSize)
{
  char *pVfsId = malloc(strlen(vfsId) + 1);
  strcpy(pVfsId, vfsId);

  // Get a reference to the default VFS
  sqlite3_vfs *pOrig = sqlite3_vfs_find(0);

  litebase_vfs.base.zName = pVfsId;

  litebase_vfs.pVfs = pOrig;
  litebase_vfs.vfsId = pVfsId;
  litebase_vfs.pageSize = pageSize;

  if (litebase_vfs.pVfs == 0)
  {
    printf("Failed to find the default VFS\n");
    return SQLITE_ERROR;
  }

  litebase_vfs.base.szOsFile = sizeof(LitebaseVFSFile) + litebase_vfs.pVfs->szOsFile;

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

      LitebaseVFS *vfs = (LitebaseVFS *)pVfs;

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

int newVfs(char *vfsId, int pageSize)
{
  assert(vfsId != NULL);
  assert(pageSize >= 512);

  sqlite3_config(SQLITE_CONFIG_LOG, logCallback, NULL);

  return register_litebase_vfs(vfsId, pageSize);
}

int litebase_get_shm(sqlite3_file *pFile, void *pp)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;
  LitebaseShm *pShm;

  if (p->pShm == NULL)
  {
    return SQLITE_IOERR_SHMLOCK;
  }

  sqlite3_mutex_enter(p->pShm->mutex);

  if (p->pShm->nRegion == 0)
  {
    sqlite3_mutex_leave(p->pShm->mutex);
    return SQLITE_OK;
  }

  for (int i = 0; i < p->pShm->nRegion; i++)
  {
    if (p->pShm->pRegions[i]->id == 0)
    {
      memcpy(pp, p->pShm->pRegions[i]->pData, 136);
      sqlite3_mutex_leave(p->pShm->mutex);
      return SQLITE_OK;
    }
  }

  sqlite3_mutex_leave(p->pShm->mutex);

  return SQLITE_OK;
}

int litebase_write_shm(sqlite3_file *pFile, void *headerData, int dataSize)
{
  LitebaseVFSFile *p = (LitebaseVFSFile *)pFile;
  LitebaseShm *pShm;
  int *aLock;

  if (p->pShm == NULL)
  {
    return SQLITE_IOERR_SHMLOCK;
  }

  pShm = p->pShm;
  aLock = pShm->aLock;

  sqlite3_mutex_enter(pShm->mutex);

  // Check if any locks are held that would prevent writing the header
  for (int i = 0; i < SQLITE_SHM_NLOCK; i++)
  {
    if (aLock[i] != 0)
    {
      sqlite3_mutex_leave(pShm->mutex);

      return SQLITE_BUSY;
    }
  }

  for (int i = 0; i < pShm->nRegion; i++)
  {
    if (pShm->pRegions[i]->id == 0)
    {
      memcpy(pShm->pRegions[i]->pData, headerData, dataSize);
      sqlite3_mutex_leave(pShm->mutex);
      return SQLITE_OK;
    }
  }

  sqlite3_mutex_leave(pShm->mutex);

  return SQLITE_OK;
}
