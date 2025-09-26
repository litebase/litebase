package http

import (
	"context"
	"errors"
	"log"
)

// Remove a member from the cluster.
func ClusterMemberControllerDestroy(ctx context.Context, request *Request) Response {
	members := request.cluster.GetMembers(false)

	ipAddress := request.Headers().Get("X-Litebase-Node")

	decryptedIp, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.EncryptionKey,
		[]byte(ipAddress),
	)

	if err != nil {
		return UnauthorizedResponse()
	}

	nodePresent := false

	for _, node := range members {
		if node.Address == decryptedIp.Value {
			nodePresent = true
			break
		}
	}

	if !nodePresent {
		return BadRequestResponse(errors.New("node is not eligible to remove a member"))
	}

	address := request.Param("address")

	if address == "" {
		log.Println("Address not provided")

		return BadRequestResponse(errors.New("address not provided"))
	}

	if address != decryptedIp.Value {
		log.Println("Unauthorized node connection attempt: ", decryptedIp.Value)

		return UnauthorizedResponse()
	}

	err = request.cluster.RemoveMember(address, false)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Member removed successfully.",
		nil,
		200,
	)
}

type ClusterMemberStoreRequest struct {
	ID      string `json:"id" validate:"required"`
	Address string `json:"address" validate:"required"`
}

// Add a new member to the cluster.
func ClusterMemberControllerStore(ctx context.Context, request *Request) Response {
	input, err := request.Input(&ClusterMemberStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	queryNodes := request.cluster.GetMembers(false)

	ipAddress := request.Headers().Get("X-Litebase-Node")

	if ipAddress == "" {
		log.Println("Unauthorized node connection attempt: ", ipAddress)
	}

	decryptedIp, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.EncryptionKey,
		[]byte(ipAddress),
	)

	if err != nil {
		return UnauthorizedResponse()
	}

	nodePresent := false

	for _, node := range queryNodes {
		if node.Address == decryptedIp.Value {
			nodePresent = true
			break
		}
	}

	if !nodePresent {
		log.Println("Node is not eligible to join the cluster: ", decryptedIp.Value)

		return BadRequestResponse(errors.New("node is not eligible to join the cluster"))
	}

	validationErrors := request.Validate(input, map[string]string{})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	err = request.cluster.AddMember(
		input.(*ClusterMemberStoreRequest).ID,
		input.(*ClusterMemberStoreRequest).Address,
	)

	if err != nil {
		log.Println("Failed to add member: ", err)

		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Member added successfully.",
		nil,
		200,
	)
}
