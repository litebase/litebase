package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ClusterElection struct {
	cancel        context.CancelFunc
	context       context.Context
	Group         string
	hasNomination bool
	mutex         sync.Mutex
	Candidates    []string
	node          *Node
	Nominee       string
	running       bool
	Seed          int64
	StartedAt     time.Time
}

// Create a new instance of a ClusterElection.
func NewClusterElection(node *Node, startTime time.Time) *ClusterElection {
	ctx, cancel := context.WithCancel(context.Background())

	return &ClusterElection{
		cancel:     cancel,
		context:    ctx,
		Candidates: []string{node.Address()},
		Group:      node.cluster.Config.NodeType,
		Nominee:    node.Address(),
		node:       node,
		Seed:       rand.Int64(),
		StartedAt:  startTime.Truncate(time.Second),
	}
}

// Add a candidate to the election along with the seed value.
func (c *ClusterElection) AddCanidate(address string, seed int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.Candidates = append(c.Candidates, address)

	if seed > c.Seed {
		c.Nominee = address
	}
}

// Get the context of the election.
func (c *ClusterElection) Context() context.Context {
	return c.context
}

// Run the election.
func (c *ClusterElection) Run() (bool, error) {
	if c.running {
		return false, fmt.Errorf("election already running")
	}

	c.running = true

	defer func() {
		c.running = false
	}()

	nominated, err := c.seekNomination()

	if err != nil {
		return false, err
	}

	if !nominated {
		return false, nil
	}

	// Write the current address to the nomination file
	success, err := c.writeNomination()

	if err != nil {
		return false, err
	}

	if !success {
		return false, nil
	}

	c.hasNomination = true

	// Confirm the election
	confirmed := c.runElectionConfirmation()

	if !confirmed {
		return false, nil
	}

	// Verify that the nomination file is still valid
	verified, err := c.verifyNomination()

	if err != nil {
		return false, err
	}

	if !verified {
		return false, nil
	}

	// Confirm the election
	confirmed = c.runElectionConfirmation()

	if !confirmed {
		return false, nil
	}

	// Verify that the nomination file is still valid
	verified, err = c.verifyNomination()

	if err != nil {
		return false, err
	}

	if !verified {
		return false, nil
	}

	err = c.node.cluster.ObjectFS().WriteFile(
		c.node.cluster.PrimaryPath(),
		[]byte(c.node.Address()),
		0644,
	)

	if err != nil {
		log.Printf("Failed to write primary file: %v", err)
		return false, err
	}

	return true, nil
}
func (n *Node) runElectionConfirmation() bool {
	nodeIdentifiers := n.cluster.NodeGroupVotingNodes()

	// If there is only one node in the group, it is the current node and the
	// election is confirmed.
	if len(nodeIdentifiers) <= 1 {
		return true
	}

	data := map[string]interface{}{
		"address":   n.Address(),
		"group":     n.cluster.Config.NodeType,
		"timestamp": n.election.StartedAt.UnixNano(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false
	}

	votes := make(chan bool, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == n.Address() {
			continue
		}

		go func(nodeIdentifier *NodeIdentifier) {
			request, err := http.NewRequestWithContext(
				n.context,
				"POST",
				fmt.Sprintf("http://%s/cluster/election/confirmation", nodeIdentifier.String()),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create confirmation election request: ", err)
				votes <- false
				return
			}

			request.Header.Set("Content-Type", "application/json")

			err = n.setInternalHeaders(request)

			if err != nil {
				log.Println("Failed to set internal headers: ", err)
				votes <- false
				return
			}

			resp, err := http.DefaultClient.Do(request)

			if err != nil {
				log.Printf(
					"Error sending election confirmation request to node %s from node %s: %s",
					nodeIdentifier.String(),
					n.Address(),
					err,
				)
			}

			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				votes <- true
			} else {
				votes <- false
			}
		}(nodeIdentifier)
	}

	// Wait for a response from each node in the group
	votesReceived := 1
	votesNeeded := len(nodeIdentifiers)/2 + 1
	timeout := time.After(3 * time.Second) // Set a timeout duration

	for i := 0; i < len(nodeIdentifiers)-1; i++ {
		select {
		case <-timeout:
			return false
		case <-n.context.Done():
			return false
		case vote := <-votes:

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

func (c *ClusterElection) runElectionConfirmation() bool {
	nodeIdentifiers := c.node.cluster.NodeGroupVotingNodes()

	// If there is only one node in the group, it is the current node and the
	// election is confirmed.
	if len(nodeIdentifiers) <= 1 {
		return true
	}

	data := map[string]interface{}{
		"address":   c.node.Address(),
		"group":     c.node.cluster.Config.NodeType,
		"timestamp": c.node.election.StartedAt.UnixNano(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false
	}

	votes := make(chan bool, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == c.node.Address() {
			continue
		}

		go func(nodeIdentifier *NodeIdentifier) {
			request, err := http.NewRequestWithContext(
				c.node.context,
				"POST",
				fmt.Sprintf("http://%s/cluster/election/confirmation", nodeIdentifier.String()),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create confirmation election request: ", err)
				votes <- false
				return
			}

			request.Header.Set("Content-Type", "application/json")

			err = c.node.setInternalHeaders(request)

			if err != nil {
				log.Println("Failed to set internal headers: ", err)
				votes <- false
				return
			}

			resp, err := http.DefaultClient.Do(request)

			if err != nil {
				log.Printf(
					"Error sending election confirmation request to node %s from node %s: %s",
					nodeIdentifier.String(),
					c.node.Address(),
					err,
				)
			}

			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				votes <- true
			} else {
				votes <- false
			}
		}(nodeIdentifier)
	}

	// Wait for a response from each node in the group
	votesReceived := 1
	votesNeeded := len(nodeIdentifiers)/2 + 1
	timeout := time.After(3 * time.Second) // Set a timeout duration

	for i := 0; i < len(nodeIdentifiers)-1; i++ {
		select {
		case <-timeout:
			return false
		case <-c.node.context.Done():
			return false
		case vote := <-votes:

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

// Stop the election.
func (c *ClusterElection) Stop() {
	if c.cancel == nil {
		return
	}

	c.cancel()
}

func (c *ClusterElection) seekNomination() (bool, error) {
	nodeIdentifiers := c.node.cluster.NodeGroupVotingNodes()

	if len(nodeIdentifiers) == 0 {
		return true, nil
	}

	// If the current node is not a voting node, return true
	var inVotingGroup bool

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == c.node.Address() {
			inVotingGroup = true
			break
		}
	}

	if !inVotingGroup {
		return false, nil
	}

	data := map[string]interface{}{
		"address":   c.node.Address(),
		"group":     c.node.cluster.Config.NodeType,
		"seed":      c.Seed,
		"timestamp": c.StartedAt.Unix(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false, err
	}

	responses := make(chan map[string]interface{}, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == c.node.Address() {
			continue
		}

		// Conduct election with other voting nodes.
		go func(nodeIdentifier *NodeIdentifier) {
			request, err := http.NewRequestWithContext(
				c.context,
				"POST",
				fmt.Sprintf("http://%s/cluster/election", nodeIdentifier.String()),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create election request: ", err)
				responses <- nil
				return
			}

			request.Header.Set("Content-Type", "application/json")

			err = c.node.setInternalHeaders(request)

			if err != nil {
				log.Println("Failed to set internal headers: ", err)
				responses <- nil
				return
			}

			resp, err := http.DefaultClient.Do(request)

			if err != nil {
				responses <- nil
				return
			}

			defer resp.Body.Close()

			var response map[string]interface{}

			err = json.NewDecoder(resp.Body).Decode(&response)

			if err != nil {
				log.Println("Failed to decode election confirmation response: ", err)
				responses <- nil
				return
			}

			responses <- response
		}(nodeIdentifier)
	}

	timeout := time.NewTimer(3 * time.Second)
	votesReceived := 1
	votesNeeded := len(nodeIdentifiers)

	for i := 0; i < len(nodeIdentifiers)-1; i++ {
		select {
		case response := <-responses:
			if response == nil {
				continue
			}

			var data map[string]interface{}
			var ok bool

			if data, ok = response["data"].(map[string]interface{}); !ok {
				continue
			}

			if nominee, ok := data["nominee"].(string); ok {
				if nominee == c.node.Address() {
					votesReceived++
				}
			}
		case <-c.context.Done():
			return false, nil
		case <-timeout.C:
			return false, fmt.Errorf("election confirmation timeout")
		}
	}

	return votesReceived >= votesNeeded, nil
}

// Read the nomination file and check if the node has already been nominated. This
// means that the node is at the top of the nomination list and the timestamp
// is within the last second.
func (c *ClusterElection) verifyNomination() (bool, error) {
	if c.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	if !c.hasNomination {
		return false, nil
	}

	// Reopen the file to read the contents
	nominationFile, err := c.node.cluster.ObjectFS().OpenFile(c.node.cluster.NominationPath(), os.O_RDONLY, 0644)

	if err != nil {
		log.Printf("Failed to reopen nomination file: %v", err)
		return false, err
	}

	defer nominationFile.Close()

	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		return false, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(nominationData))

	// Check if the node has already been nominated
	if scanner.Scan() {
		firstLine := scanner.Text()

		if strings.HasPrefix(firstLine, c.node.Address()) {
			timestamp := strings.Split(firstLine, " ")[1]

			timestampInt, err := strconv.ParseInt(timestamp, 10, 64)

			if err != nil {
				return false, err
			}

			// Parse the timestamp
			if time.Now().UnixNano()-timestampInt < time.Second.Nanoseconds() {
				return true, nil
			}
		}
	}

	return false, nil
}

// Write the nodes address to the nomination file in attempt to nominate itself
// as the primary node.
func (c *ClusterElection) writeNomination() (bool, error) {
	if c.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	// Attempt to open the nomination file with exclusive lock
openNomination:
	nominationFile, err := c.node.cluster.ObjectFS().OpenFile(c.node.cluster.NominationPath(), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to open nomination file: %v", err)
			return false, err
		}

		// Retry if the file does not exist
		err = c.node.cluster.ObjectFS().MkdirAll(filepath.Dir(c.node.cluster.NominationPath()), 0755)

		if err != nil {
			log.Printf("Failed to create nomination directory: %v", err)
			return false, err
		}

		goto openNomination
	}

	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		log.Printf("Failed to read nomination file: %v", err)
		return false, err
	}

	address := c.node.Address()
	timestamp := time.Now().UnixNano()
	entry := fmt.Sprintf("%s %d\n", address, timestamp)

	// Read the nomination file and check if it is empty or does not contain
	// this node's address in addition to the timestamp being past 1 second.
	if len(nominationData) == 0 {
		_, err = nominationFile.WriteString(entry)

		if err != nil {
			log.Printf("Failed to write to nomination file: %v", err)
			return false, err
		}
	} else {
		// File is not empty and does not contain this node's address
		// Implement logic to determine if this node should still become primary based on timestamps or other criteria
		scanner := bufio.NewScanner(bytes.NewReader(nominationData))

		// Check if the node has already been nominated
		if scanner.Scan() {
			firstLine := scanner.Text()

			timestamp := strings.Split(firstLine, " ")[1]

			timestampInt, err := strconv.ParseInt(timestamp, 10, 64)

			if err != nil {
				return false, err
			}

			if time.Now().UnixNano()-timestampInt < time.Second.Nanoseconds() {
				return false, nil
			}
		}

		err := nominationFile.Truncate(0)

		if err != nil {
			log.Printf("Failed to truncate nomination file: %v", err)
			return false, err
		}

		_, err = nominationFile.WriteString(entry)

		if err != nil {
			log.Printf("Failed to write to nomination file: %v", err)
			return false, err
		}
	}

	err = nominationFile.Close()

	if err != nil {
		return false, err
	}

	return true, nil
}
