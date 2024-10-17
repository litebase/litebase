package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

type ClusterElection struct {
	cancel     context.CancelFunc
	context    context.Context
	mutex      sync.Mutex
	Candidates []string
	Group      string
	node       *Node
	Nominee    string
	running    bool
	Seed       int64
	StartedAt  time.Time
}

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

func (c *ClusterElection) AddCanidate(address string, seed int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.Candidates = append(c.Candidates, address)

	if seed > c.Seed {
		c.Nominee = address
	}
}

func (c *ClusterElection) Stop() {
	if c.cancel == nil {
		return
	}

	c.cancel()
}

func (c *ClusterElection) Run() (bool, error) {
	if c.running {
		return false, fmt.Errorf("election already running")
	}

	c.running = true

	defer func() {
		c.running = false
	}()

	nodeIdentifiers := c.node.cluster.NodeGroupVotingNodes()

	if len(nodeIdentifiers) == 0 {
		return true, nil
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

		go func(nodeIdentifier *NodeIdentifier) {
			request, err := http.NewRequestWithContext(
				c.context,
				"POST",
				fmt.Sprintf("http://%s/cluster/election", nodeIdentifier.String()),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create confirmation election request: ", err)
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

			if nominee, ok := response["data"].(map[string]interface{})["nominee"].(string); ok {
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
