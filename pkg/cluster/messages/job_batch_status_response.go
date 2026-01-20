package messages

import "time"

type JobBatchStatusResponse struct {
	BatchID       int64      `json:"batch_id"`
	Name          string     `json:"name"`
	TotalJobs     int        `json:"total_jobs"`
	PendingJobs   int        `json:"pending_jobs"`
	CompletedJobs int        `json:"completed_jobs"`
	FailedJobs    int        `json:"failed_jobs"`
	Progress      int        `json:"progress"`
	IsFinished    bool       `json:"is_finished"`
	CreatedAt     time.Time  `json:"created_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}
