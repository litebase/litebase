package http

import (
	"log"
)

// Remove a member from the cluster.
func ClusterMemberDestroyController(request *Request) Response {
	members := request.cluster.GetMembers(false)

	ipAddress := request.Headers().Get("X-Lbdb-Node")

	decryptedIp, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.Signature,
		[]byte(ipAddress),
	)

	if err != nil {
		return Response{
			StatusCode: 401,
		}
	}

	nodePresent := false

	for _, node := range members {
		if node.Address == decryptedIp.Value {
			nodePresent = true
			break
		}
	}

	if !nodePresent {
		return Response{
			StatusCode: 400,
		}
	}

	address := request.Param("address")

	if address == "" {
		log.Println("Address not provided")

		return Response{
			StatusCode: 400,
		}
	}

	if address != decryptedIp.Value {
		log.Println("Unauthorized node connection attempt: ", decryptedIp.Value)

		return Response{
			StatusCode: 401,
		}
	}

	request.cluster.RemoveMember(address, false)

	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}

type ClusterMemberStoreRequest struct {
	ID      string `json:"id" validate:"required"`
	Address string `json:"address" validate:"required"`
}

// Add a new member to the cluster.
func ClusterMemberStoreController(request *Request) Response {
	input, err := request.Input(&ClusterMemberStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	queryNodes := request.cluster.GetMembers(false)

	ipAddress := request.Headers().Get("X-Lbdb-Node")

	if ipAddress == "" {
		log.Println("Unauthorized node connection attempt: ", ipAddress)
	}

	decryptedIp, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.Signature,
		[]byte(ipAddress),
	)

	if err != nil {
		return Response{
			StatusCode: 401,
		}
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

		return Response{
			StatusCode: 400,
		}
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

		return Response{
			StatusCode: 500,
		}
	}

	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}
