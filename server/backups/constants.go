package backups

import "os"

const BACKUP_DIR = "backups"
const BACKUP_OBJECT_DIR = "objects"
const RESTORE_POINTS_DIR = "restore_points"
const SNAPSHOT_LOG_FLAGS = os.O_RDWR | os.O_CREATE | os.O_APPEND
