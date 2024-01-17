package timer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"litebasedb/internal/config"
)

var ClockStart time.Time

type Timer struct {
	description string
	timestart   time.Time
}

var TimersMutex sync.Mutex
var Timers = map[int64]Timer{}

func Clock() {
	if !config.Get().Debug {
		return
	}

	ClockStart = time.Now()
}

func Start(description string) int64 {
	if !config.Get().Debug {
		return 0
	}

	timestamp := time.Now().UnixNano()

	TimersMutex.Lock()
	defer TimersMutex.Unlock()

	Timers[timestamp] = Timer{
		description: description,
		timestart:   time.Now(),
	}

	return timestamp
}

func Stop(timestamp int64) {
	if !config.Get().Debug {
		return
	}

	TimersMutex.Lock()
	timer, ok := Timers[timestamp]
	TimersMutex.Unlock()

	if !ok {
		return
	}

	log.Println(
		fmt.Sprintf("[%s]:", timer.description),
		fmt.Sprintf("(%s / %s)", time.Since(timer.timestart), time.Since(ClockStart)),
	)

	TimersMutex.Lock()
	delete(Timers, timestamp)
	TimersMutex.Unlock()
}
