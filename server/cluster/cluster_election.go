package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"syscall"
	"time"
)

const (
	ElectionDeadline  = 3 * time.Second
	ElectionRetryWait = 1 * time.Second
)

var (
	ErrElectionAlreadyRunning     = errors.New("election already running")
	ErrMustWaitBeforeNextElection = errors.New("must wait before starting the next election")
)

type ClusterElection struct {
	cancel    context.CancelFunc
	Candidate uint64
	EndsAt    time.Time
	context   context.Context
	node      *Node
	Seed      int64
	StartedAt time.Time
	StoppedAt time.Time
}

// Create a new ClusterElection instance for the given node.
func NewClusterElection(node *Node) *ClusterElection {
	ctx, cancel := context.WithCancel(node.Context())
	startedAt := time.Now()

	return &ClusterElection{
		cancel:    cancel,
		Candidate: node.ID,
		context:   ctx,
		EndsAt:    startedAt.Add(ElectionDeadline),
		node:      node,
		Seed:      rand.Int64(),
		StartedAt: startedAt,
	}
}

// Expired checks if the election has expired based on the current time.
func (ce *ClusterElection) Expired() bool {
	return time.Now().After(ce.EndsAt)
}

// Send requests to other nodes to get their votes for the current node to
// become the cluster leader.
func (ce *ClusterElection) proposeLeadership() bool {
	if ce.context == nil || ce.context.Err() != nil {
		return false
	}

	votingNodes := ce.node.Cluster.Nodes()

	if len(votingNodes) == 1 && votingNodes[0].Address == ce.node.address {
		return true
	}

	data := map[string]any{
		"candidate":  ce.node.ID,
		"seed":       ce.Seed,
		"started_at": ce.StartedAt.UnixNano(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		slog.Debug("Failed to marshal election data", "error", err)
		return false
	}

	votesNeeded := len(votingNodes) - 1
	voteResponses := votesNeeded
	votes := make(chan bool, voteResponses) // -1 because we don't vote for ourselves
	votesReceived := 1

	for _, nodeAddress := range votingNodes {
		if nodeAddress.Address == ce.node.address {
			continue
		}
		go func(nodeAddress string) {
			requestCtx, cancel := context.WithTimeout(ce.node.context, 3*time.Second)
			defer cancel()

			request, err := http.NewRequestWithContext(
				requestCtx,
				"POST",
				fmt.Sprintf("http://%s/cluster/election", nodeAddress),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create confirmation election request: ", err)
				votes <- false
				return
			}

			request.Header.Set("Content-Type", "application/json")

			err = ce.node.setInternalHeaders(request)

			if err != nil {
				log.Println("Failed to set internal headers: ", err)
				votes <- false
				return
			}

			resp, err := http.DefaultClient.Do(request)

			if err != nil {
				log.Printf(
					"Error sending election request to node %s from node: %s",
					nodeAddress,
					err,
				)
			}

			if resp == nil {
				log.Printf("No response from node %s during election", nodeAddress)
				votes <- false
				return
			}

			if resp.Body != nil {
				defer resp.Body.Close()
			}

			jsonData := make(map[string]any)

			err = json.NewDecoder(resp.Body).Decode(&jsonData)

			if err != nil {
				log.Printf("Error decoding response from node %s: %v", nodeAddress, err)
				votes <- false
				return
			}

			if resp.StatusCode == http.StatusOK {
				votes <- true
			} else {
				votes <- false
			}
		}(nodeAddress.Address)
	}

	// Wait for a response from each node in the voting group
	timeout := time.After(3 * time.Second) // Set a timeout duration

	for voteResponses > 0 {
		select {
		case <-timeout:
			return false
		case vote := <-votes:
			voteResponses--

			if vote {
				votesReceived++
			}

			if votesReceived == votesNeeded {
				return true
			}
		}
	}

	return false
}

// Run the election process. Lock the primary files, send a leadership proposal
// to other nodes in the cluster. If successful, the current node becomes leader.
func (ce *ClusterElection) run() (bool, error) {
	defer ce.Stop()

	// Add a random sleep to avoid simultaneous elections
	sleepDuration := time.Duration(100+rand.IntN(901)) * time.Millisecond
	time.Sleep(sleepDuration)

	if ce.node.Context().Err() != nil {
		return false, ce.node.Context().Err()
	}

	// Try to lock the primary file
	primaryFile, err := ce.node.Cluster.NetworkFS().Open(ce.node.Cluster.PrimaryPath())

	if err != nil {
		log.Printf("Failed to open primary file: %v", err)
		return false, err
	}

	// Attempt to lock the primary file using syscall.Flock
	file := primaryFile.(*os.File)

	locked := false

	defer func() {
		if locked {
			syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		}
		primaryFile.Close()
	}()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			log.Printf("Primary file is locked by another process: %v", err)
			return false, nil // Not an error, just not elected
		}

		return false, err
	}

	locked = true

	// Propose leadership to other nodes. If other nodes accept, continue
	elected := ce.proposeLeadership()

	if !elected {
		return false, nil
	}

	// Write the current node's address to the primary file
	err = ce.node.Cluster.NetworkFS().WriteFile(
		ce.node.Cluster.PrimaryPath(),
		[]byte(ce.node.address),
		0644,
	)

	if err != nil {
		log.Printf("Failed to write primary file: %v", err)
		return false, err
	}

	return true, nil
}

// Check if the election is running.
func (ce *ClusterElection) Running() bool {
	return ce.context.Err() == nil && ce.StoppedAt.IsZero()
}

// Stop the election process and mark the time it was stopped.
func (ce *ClusterElection) Stop() {
	ce.cancel()
	ce.StoppedAt = time.Now()
}

// Check if the election has been stopped.
func (ce *ClusterElection) Stopped() bool {
	return !ce.StoppedAt.IsZero()
}
