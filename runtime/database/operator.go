package database

import (
	"litebasedb/runtime/auth"
	"litebasedb/runtime/backups"
	"litebasedb/runtime/config"
	"litebasedb/runtime/sqlite3"
	"log"
)

type DatabaseOperator struct {
	isWriting      bool
	isTransmitting bool
	wal            *DatabaseWAL
}

func NewOperator(wal *DatabaseWAL) *DatabaseOperator {
	return &DatabaseOperator{
		isWriting: false,
		wal:       wal,
	}
}

func (o *DatabaseOperator) Monitor(isRead bool, callback func() (sqlite3.Result, error)) (sqlite3.Result, error) {
	o.isWriting = !isRead
	result, err := callback()
	o.isWriting = false

	return result, err
}

func (o *DatabaseOperator) IsWriting() bool {
	return o.isWriting
}

func (o *DatabaseOperator) Record() int {
	branchUuid := config.Get("branch_uuid")
	databaseUuid := config.Get("database_uuid")
	settings, err := auth.SecretsManager().GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		log.Fatal(err)
		return 0
	}

	branchSettings, hasBranchSettings := settings["branchSettings"].(map[string]interface{})

	if !hasBranchSettings {
		return 0
	}

	backupSettings, hasBackupSettings := branchSettings["backups"]

	if !hasBackupSettings || !backupSettings.(map[string]interface{})["enabled"].(bool) {
		return 0
	}

	incrementalBackupSettings, hasIncrementalBackupSettings := backupSettings.(map[string]interface{})["incremental_backups"]

	if !hasIncrementalBackupSettings || !incrementalBackupSettings.(map[string]interface{})["enabled"].(bool) {
		return 0
	}

	if len(o.wal.ChangedPages) > 0 {
		backups.SaveRestorePoint(databaseUuid, branchUuid, o.wal.ChangedPages)
	}

	return len(o.wal.ChangedPages)
}

func (o *DatabaseOperator) Transmit() {
	o.isTransmitting = true
	o.wal.CheckPoint()
	o.isTransmitting = false
}

func (o *DatabaseOperator) Transmitting() bool {
	return o.isTransmitting
}
