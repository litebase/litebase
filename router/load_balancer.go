package router

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type LoadBalancer struct {
	currentTarget int64
	targets       []*Target
}

func NewLoadBalancer() *LoadBalancer {
	lb := &LoadBalancer{
		targets: []*Target{},
	}

	lb.setTargets()

	go func() {
		lb.getTargets()
		lb.runHealthChecks()
	}()

	return lb
}

func (lb *LoadBalancer) getTargets() {
	timer := time.NewTicker(3 * time.Second)

	for range timer.C {
		lb.setTargets()
	}
}

// HealthCheck pings the backends and update the status
func (lb *LoadBalancer) HealthCheck() {
	for _, target := range lb.targets {
		status := "up"
		alive := lb.checkTarget(target)
		target.SetAlive(alive)

		if !alive {
			status = "down"
		}

		log.Printf("%s [%s]\n", target.URL, status)
	}
}

func (lb *LoadBalancer) NextTarget() (*Target, error) {
	if len(lb.targets) == 0 {
		return nil, fmt.Errorf("no targets available")
	}

	next := lb.NextIndex()

	l := len(lb.targets) + next // start from next and move a full cycle

	for i := next; i < l; i++ {
		index := i % len(lb.targets) // take an index by modding with length

		// if we have an alive backend, use it and store if its not the original one
		if lb.targets[index].IsAlive() {
			if i != next {
				atomic.StoreInt64(&lb.currentTarget, int64(index)) // mark the current one
			}

			return lb.targets[index], nil
		}
	}

	return nil, fmt.Errorf("no targets available")
}

func (lb *LoadBalancer) NextIndex() int {
	return int(atomic.AddInt64(&lb.currentTarget, int64(1)) % int64(len(lb.targets)))
}

func (lb *LoadBalancer) setTargets() {
	targets := []*Target{}

	entries, err := os.ReadDir(fmt.Sprintf("%s/%s", os.Getenv("LITEBASE_DATA_PATH"), "/_nodes"))

	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No targets available")
		}

		log.Println(err)

		return
	}

	for _, entry := range entries {
		parts := strings.Split(entry.Name(), ":")

		if len(parts) != 2 {
			continue
		}

		targets = append(targets, NewTarget(lb, parts[0], parts[1]))
	}

	lb.targets = targets
}

func (lb *LoadBalancer) checkTarget(target *Target) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", target.URL.Host, timeout)

	if err != nil {
		log.Println("Service unreachable, error: ", err)
		return false
	}

	defer conn.Close()

	return true
}

func (lb *LoadBalancer) Handle(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	log.Println("Attempts: ", attempts)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	target, err := lb.NextTarget()
	log.Println("Target: ", target)
	if err != nil {
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	target.ServeHTTP(w, r)
}

func (lb *LoadBalancer) runHealthChecks() {
	t := time.NewTicker(time.Minute * 2)

	for {
		select {
		case <-t.C:
			log.Println("Starting health check...")
			lb.HealthCheck()
			log.Println("Health check completed")
		}
	}
}
