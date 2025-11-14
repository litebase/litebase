package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
)

type ClusterElectionRequest struct {
	Candidate string `json:"candidate" validate:"required"`
	Seed      int64  `json:"seed" validate:"required"`
	StartedAt int64  `json:"startedAt" validate:"required"`
}

// Handle a cluster election request
func ClusterElectionControllerStore(ctx context.Context, request *Request) Response {
	input, err := request.Input(&ClusterElectionRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	req, ok := input.(*ClusterElectionRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"candidate.required":  "The candidate field is required",
		"seed.required":       "The seed field is required",
		"started_at.required": "The started_at field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	if request.cluster.Node().ID == req.Candidate {
		return BadRequestResponse(fmt.Errorf("cannot start election, candidate is the same as the current node"))
	}

	// If the current node is the primary, the lease is up to date return error
	if request.cluster.Node().IsPrimary() &&
		request.cluster.Node().Lease().IsUpToDate() {
		return BadRequestResponse(fmt.Errorf("cannot start election, current node is primary and lease is up to date"))
	}

	// Check if the node has running elections in progress
	if request.cluster.Node().Election != nil && request.cluster.Node().Election.Running() {
		if request.cluster.Node().Election.Seed > req.Seed {
			return BadRequestResponse(fmt.Errorf("election with a higher seed is already running"))
		} else {
			// Stop the current election and start a new one
			request.cluster.Node().Election.Stop()
		}
	}

	// Check for peer elections that are running
	if request.cluster.Node().HasPeerElectionRunning() {
		hasRunningPeerElection := len(request.cluster.Node().PeerElections()) > 0

		if hasRunningPeerElection {
			return BadRequestResponse(fmt.Errorf("a peer election is already running"))
		}
	}

	request.cluster.Node().AddPeerElection(&cluster.ClusterElection{
		Candidate: req.Candidate,
		Seed:      req.Seed,
		StartedAt: time.Unix(0, req.StartedAt).UTC(),
	})

	return SuccessResponse("Election acknowledged", map[string]any{
		"candidate": req.Candidate,
		"votedAt":   time.Now().UTC().Unix(),
	}, 200)
}
