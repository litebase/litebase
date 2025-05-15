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
	address, _ := node.Address()

	return &ClusterElection{
		Candidates: []string{address},
		Group:      node.Cluster.Config.NodeType,
		Nominee:    address,
		node:       node,
		StartedAt:  startTime.Truncate(time.Second),
	}
}

// Add a candidate to the election along with the seed value.
func (c *ClusterElection) AddCandidate(address string, seed int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.Candidates = append(c.Candidates, address)

	if seed > c.Seed {
		c.Nominee = address

		if c.running {
			c.Stop()
		}
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

	c.context, c.cancel = context.WithCancel(context.Background())
	c.Seed = rand.Int64()

	nominated, err := c.seekNomination()

	if err != nil {
		log.Printf("Failed to seek nomination: %v", err)
		return false, err
	}

	if !nominated {
		return false, nil
	}

	// Write the current address to the nomination file
	success, err := c.writeNomination()

	if err != nil {
		log.Printf("Failed to write nomination: %v", err)
		return false, err
	}

	// log.Println("Wrote nomination: ", success)
	if !success {
		return false, nil
	}

	c.hasNomination = true

	if !c.running {
		return false, nil
	}

	// Confirm the election
	confirmed := c.runElectionConfirmation()

	if !confirmed {
		return false, nil
	}

	if !c.running {
		return false, nil
	}

	// Verify that the nomination file is still valid
	verified, err := c.verifyNomination()

	if err != nil {
		return false, err
	}

	if !verified || !c.running {
		return false, nil
	}

	// Confirm the election
	confirmed = c.runElectionConfirmation()

	if !confirmed || !c.running {
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

	address, _ := c.node.Address()

	err = c.node.Cluster.NetworkFS().WriteFile(
		c.node.Cluster.PrimaryPath(),
		[]byte(address),
		0644,
	)

	if err != nil {
		log.Printf("Failed to write primary file: %v", err)
		return false, err
	}

	return true, nil
}

func (c *ClusterElection) runElectionConfirmation() bool {
	if c.context.Err() != nil {
		return false
	}

	address, _ := c.node.Address()

	nodeIdentifiers := c.node.Cluster.NodeGroupVotingNodes()

	// If there is only one node in the group, it is the current node and the
	// election is confirmed.
	if len(nodeIdentifiers) <= 1 {
		return true
	}

	data := map[string]any{
		"address":   address,
		"group":     c.node.Cluster.Config.NodeType,
		"timestamp": c.node.election.StartedAt.UnixNano(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false
	}

	votes := make(chan bool, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == address {
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
				// log.Printf(
				// 	"Error sending election confirmation request to node %s from node %s: %s",
				// 	nodeIdentifier.String(),
				// 	c.node.Address(),
				// 	err,
				// )
			}

			if resp == nil {
				votes <- false
				return
			}

			if resp.Body != nil {
				defer resp.Body.Close()
			}

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

	for range len(nodeIdentifiers) - 1 {
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

func (c *ClusterElection) seekNomination() (bool, error) {
	if c.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	address, _ := c.node.Address()

	nodeIdentifiers := c.node.Cluster.NodeGroupVotingNodes()

	if len(nodeIdentifiers) == 0 {
		return true, nil
	}

	// If the current node is not a voting node, return true
	var inVotingGroup bool

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == address {
			inVotingGroup = true
			break
		}
	}

	if !inVotingGroup {
		return false, nil
	}

	data := map[string]any{
		"address":   address,
		"group":     c.node.Cluster.Config.NodeType,
		"seed":      c.Seed,
		"timestamp": c.StartedAt.Unix(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false, err
	}

	responses := make(chan map[string]any, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == address {
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

			var response map[string]any

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

	for range len(nodeIdentifiers) - 1 {
		select {
		case response := <-responses:
			if response == nil {
				continue
			}

			var data map[string]any
			var ok bool

			if data, ok = response["data"].(map[string]any); !ok {
				continue
			}

			if nominee, ok := data["nominee"].(string); ok {
				if nominee == address {
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

// Stop the election.
func (c *ClusterElection) Stop() {
	if c.cancel == nil {
		return
	}

	c.running = false

	c.cancel()
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

	address, _ := c.node.Address()

	// Reopen the file to read the contents
	nominationFile, err := c.node.Cluster.NetworkFS().OpenFile(c.node.Cluster.NominationPath(), os.O_RDONLY, 0644)

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

		if strings.HasPrefix(firstLine, address) {
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

openNomination:
	nominationFile, err := c.node.Cluster.NetworkFS().OpenFile(c.node.Cluster.NominationPath(), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to open nomination file: %v", err)
			return false, err
		}

		// Retry if the file does not exist
		err = c.node.Cluster.NetworkFS().MkdirAll(filepath.Dir(c.node.Cluster.NominationPath()), 0755)

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

	nodeAddress, _ := c.node.Address()
	timestamp := time.Now().UnixNano()
	entry := fmt.Sprintf("%s %d\n", nodeAddress, timestamp)

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
			parts := strings.Split(firstLine, " ")

			address := parts[0]
			// Check if the address is the same as this node's address
			timestamp := parts[1]

			timestampInt, err := strconv.ParseInt(timestamp, 10, 64)

			if err != nil {
				log.Println("Failed to parse timestamp: ", err)
				return false, err
			}

			if nodeAddress != address &&
				time.Now().UnixNano()-timestampInt < time.Millisecond.Nanoseconds() {
				log.Println("Node already nominated: ", address, address)
				return false, nil
			}
		}

		err := nominationFile.Truncate(0)

		if err != nil {
			log.Printf("Failed to truncate nomination file: %v", err)
			return false, err
		}

		_, err = nominationFile.Seek(0, io.SeekStart)

		if err != nil {
			log.Printf("Failed to seek nomination file: %v", err)
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
		log.Println("Failed to close nomination file: ", err)
		return false, err
	}

	return true, nil
}
