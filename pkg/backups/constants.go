package backups

import "os"

const BACKUP_DIR = "backups"
const BACKUP_MAX_PART_SIZE = 1024 * 1024 * 1024 * 100 // 100 GB
const BACKUP_OBJECT_DIR = "objects"
const SNAPSHOT_LOG_FLAGS = os.O_RDWR | os.O_CREATE | os.O_APPEND
