package http

type ClusterElectionMessage struct {
	Address   string `json:"address" validate:"required"`
	Group     string `json:"group" validate:"required"`
	Seed      int64  `json:"seed" validate:"required"`
	Timestamp int64  `json:"timestamp" validate:"required"`
}

func ClusterElectionController(request *Request) Response {
	input, err := request.Input(&ClusterElectionMessage{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"address.requried":   "The address field is required",
		"group.required":     "The group field is required",
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
				"errors": "Invalid input",
			},
		}
	}

	// Check that the group is the same as the node type
	if message.Group != request.cluster.Config.NodeType {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"errors": "Invalid group",
			},
		}
	}

	election := request.cluster.Node().Election()

	election.AddCandidate(
		input.(*ClusterElectionMessage).Address,
		input.(*ClusterElectionMessage).Seed,
	)

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"message": "Election started",
			"data": map[string]any{
				"nominee": election.Nominee,
			},
		},
	}
}
