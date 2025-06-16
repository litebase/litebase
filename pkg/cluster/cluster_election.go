package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"math/big"
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
	Candidate string
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
	startedAt := time.Now().UTC()

	randInt64, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))

	if err != nil {
		log.Println("Failed to generate random seed for election:", err)
		randInt64 = big.NewInt(0) // Fallback to zero if random generation fails
	}

	return &ClusterElection{
		cancel:    cancel,
		Candidate: node.ID,
		context:   ctx,
		EndsAt:    startedAt.Add(ElectionDeadline),
		node:      node,
		Seed:      randInt64.Int64(),
		StartedAt: startedAt,
	}
}

func (ce *ClusterElection) Context() context.Context {
	return ce.context
}

// Expired checks if the election has expired based on the current time.
func (ce *ClusterElection) Expired() bool {
	return time.Now().UTC().After(ce.EndsAt)
}

// Send requests to other nodes to get their votes for the current node to
// become the cluster leader.
func (ce *ClusterElection) proposeLeadership() bool {
	if ce.context == nil || ce.context.Err() != nil {
		return false
	}
	// Refresh the cluster members to ensure we have the latest information
	ce.node.Cluster.GetMembers(false)

	votingNodes := ce.node.Cluster.Nodes()

	if len(votingNodes) <= 1 {
		// If there are no other nodes or only ourselves, we win by default
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

	// Calculate votes needed for majority (more than half of all nodes)
	totalNodes := len(votingNodes)
	votesNeeded := (totalNodes / 2) + 1

	// Count other nodes (excluding current node)
	otherNodes := 0
	for _, nodeAddress := range votingNodes {
		if nodeAddress.Address != ce.node.address {
			otherNodes++
		}
	}

	// If no other nodes, we win by default
	if otherNodes == 0 {
		return true
	}

	votes := make(chan bool, otherNodes)
	votesReceived := 1 // Start with 1 (our own vote)

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
				slog.Debug(
					"Error sending election request",
					"address", nodeAddress,
					"error", err,
				)
			}

			if resp == nil {
				votes <- false
				return
			}

			if resp.Body != nil {
				defer resp.Body.Close()
			}

			jsonData := make(map[string]any)

			err = json.NewDecoder(resp.Body).Decode(&jsonData)

			if err != nil {
				slog.Debug("Error decoding response from node", "address", nodeAddress, "error", err)
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
	responsesRemaining := otherNodes

	for responsesRemaining > 0 {
		select {
		case <-timeout:
			return false
		case vote := <-votes:
			responsesRemaining--

			if vote {
				votesReceived++
			}

			if votesReceived >= votesNeeded {
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

	if !ce.node.Cluster.IsSingleNodeCluster() {
		// Add a random sleep to avoid simultaneous elections
		randInt64, err := rand.Int(rand.Reader, big.NewInt(901))

		if err != nil {
			return false, err
		}

		sleepDuration := time.Duration(100+randInt64.Int64()) * time.Millisecond

		time.Sleep(sleepDuration)
	}

	if ce.node.Context().Err() != nil {
		return false, ce.node.Context().Err()
	}

	// Try to lock the primary file
	primaryFile, err := ce.node.Cluster.NetworkFS().Open(ce.node.Cluster.PrimaryPath())

	if err != nil {
		slog.Debug("Failed to open primary file", "error", err)
		return false, err
	}

	// Attempt to lock the primary file using syscall.Flock
	file := primaryFile.(*os.File)

	locked := false

	defer func() {
		if locked {
			err = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

			if err != nil {
				slog.Debug("Failed to unlock primary file", "error", err)
			}
		}

		err = primaryFile.Close()

		if err != nil {
			slog.Debug("Failed to close primary file", "error", err)
		}
	}()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			slog.Debug("Primary file is locked by another process", "error", err)
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
		0600,
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
	ce.StoppedAt = time.Now().UTC()
}

// Check if the election has been stopped.
func (ce *ClusterElection) Stopped() bool {
	return !ce.StoppedAt.IsZero()
}
