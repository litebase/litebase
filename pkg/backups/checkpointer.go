package backups

type Checkpointer interface {
	CheckpointBarrier(func() error) error
}
