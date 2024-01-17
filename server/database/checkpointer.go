package database

import "time"

type Checkpointer struct {
	cancel chan struct{}
	ticker *time.Ticker
}

func NewCheckpointer(checkpointHook func()) *Checkpointer {
	checkpointer := &Checkpointer{
		ticker: time.NewTicker(1 * time.Second),
	}

	checkpointer.start(checkpointHook)

	return checkpointer
}

func (c *Checkpointer) start(checkpointHook func()) {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				checkpointHook()
			case <-c.cancel:
				c.ticker.Stop()
				return
			}
		}
	}()
}

func (c *Checkpointer) Stop() {
	c.cancel <- struct{}{}
	close(c.cancel)
}
