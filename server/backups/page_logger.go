package backups

type PageLogger struct {
	databaseUuid string
	branchUuid   string
}

func NewPageLogger(databaseUuid, branchUuid string) *PageLogger {
	return &PageLogger{
		databaseUuid: databaseUuid,
		branchUuid:   branchUuid,
	}
}

func (p *PageLogger) Log(pageNumber uint32, timstamp uint64, data []byte) error {
	pageLog, err := OpenPageLog(p.databaseUuid, p.branchUuid, pageNumber)

	if err != nil {
		return err
	}

	defer pageLog.Close()

	return pageLog.Append(NewPageLogEntry(pageNumber, timstamp, data))
}
