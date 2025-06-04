package http

import (
	"time"

	"github.com/litebase/litebase/server/cluster"
)

type ClusterElectionMessage struct {
	Candidate uint64 `json:"candidate" validate:"required"`
	Seed      int64  `json:"seed" validate:"required"`
	StartedAt int64  `json:"started_at" validate:"required"`
}

func ClusterElectionController(request *Request) Response {
	input, err := request.Input(&ClusterElectionMessage{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"address.required":   "The address field is required",
		"id.required":        "The id field is required",
		"seed.required":      "The seed field is required",
		"timestamp.required": "The timestamp field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	message, ok := input.(*ClusterElectionMessage)

	if !ok {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "Invalid input",
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
		if request.cluster.Node().Election.Seed > message.Seed {
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
		Candidate: message.Candidate,
		Seed:      message.Seed,
		StartedAt: time.Unix(0, message.StartedAt),
	})

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"message": "Election acknowledged",
			"data": map[string]any{
				"candidate": message.Candidate,
				"voted_at":  time.Now().Unix(),
			},
		},
	}
}
