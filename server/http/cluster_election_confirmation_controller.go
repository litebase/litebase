package http

type ClusterElectionConfirmationMessage struct {
	Address   string `json:"address" validate:"required"`
	Group     string `json:"group" validate:"required"`
	Timestamp int64  `json:"timestamp" validate:"required"`
}

func ClusterElectionConfirmationController(request *Request) Response {
	input, err := request.Input(&ClusterElectionConfirmationMessage{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"address.requried":   "The address field is required",
		"group.required":     "The group field is required",
		"timestamp.required": "The timestamp field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	if input.(*ClusterElectionConfirmationMessage).Group != request.cluster.Config.NodeType {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "Invalid group",
			},
		}
	}

	address := input.(*ClusterElectionConfirmationMessage).Address
	confirmed, err := request.cluster.Node().VerifyElectionConfirmation(address)

	if err != nil {
		return ServerErrorResponse(err)
	}

	if !confirmed {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "Invalid confirmation",
			},
		}
	}

	// Check the current election to ensure this node is the nominee

	election := request.cluster.Node().Election()

	if election.Nominee != address {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "Invalid nominee",
			},
		}
	}

	defer election.Stop()

	return Response{
		StatusCode: 200,
	}
}
