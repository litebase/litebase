package http

import (
	"time"

	"github.com/litebase/litebase/server/cluster"
)

type ClusterElectionRequest struct {
	Candidate string `json:"candidate" validate:"required"`
	Seed      int64  `json:"seed" validate:"required"`
	StartedAt int64  `json:"started_at" validate:"required"`
}

func ClusterElectionController(request *Request) Response {
	input, err := request.Input(&ClusterElectionRequest{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"candidate.required":  "The candidate field is required",
		"seed.required":       "The seed field is required",
		"started_at.required": "The started_at field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	if request.cluster.Node().ID == input.(*ClusterElectionRequest).Candidate {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "Cannot start election, candidate is the same as the current node",
			},
		}
	}

	// If the current node is the primary, the lease is up to date return error
	if request.cluster.Node().IsPrimary() &&
		request.cluster.Node().Lease().IsUpToDate() {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "Cannot start election, current node is primary and lease is up to date",
			},
		}
	}

	// Check if the node has running elections in progress
	if request.cluster.Node().Election != nil && request.cluster.Node().Election.Running() {
		if request.cluster.Node().Election.Seed > input.(*ClusterElectionRequest).Seed {
			return Response{
				StatusCode: 400,
				Body: map[string]any{
					"message": "Election with a higher seed is already running",
				},
			}
		} else {
			// Stop the current election and start a new one
			request.cluster.Node().Election.Stop()
		}
	}

	// Check for peer elections that are running
	if request.cluster.Node().HasPeerElectionRunning() {
		hasRunningPeerElection := len(request.cluster.Node().PeerElections()) > 0

		if hasRunningPeerElection {
			return Response{
				StatusCode: 400,
				Body: map[string]any{
					"message": "A peer election is already running",
				},
			}
		}
	}

	request.cluster.Node().AddPeerElection(&cluster.ClusterElection{
		Candidate: input.(*ClusterElectionRequest).Candidate,
		Seed:      input.(*ClusterElectionRequest).Seed,
		StartedAt: time.Unix(0, input.(*ClusterElectionRequest).StartedAt),
	})

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"message": "Election acknowledged",
			"data": map[string]any{
				"candidate": input.(*ClusterElectionRequest).Candidate,
				"voted_at":  time.Now().Unix(),
			},
		},
	}
}
