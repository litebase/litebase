package http

import (
	"log"
	"strconv"
)

func ClusterMemberDestroyController(request *Request) Response {
	queryNodes := request.cluster.GetMembers(true)

	ipAddress := request.Headers().Get("X-Lbdb-Node")

	decryptedIp, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.Signature,
		[]byte(ipAddress),
	)

	if err != nil {
		log.Println("Unauthorized node connection attempt:", err)

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

func ClusterMemberStoreController(request *Request) Response {
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

	IDUint64, err := strconv.ParseUint(request.Get("id").(string), 10, 64)

	if err != nil {
		log.Println("Failed to parse ID: ", err)
		return Response{
			StatusCode: 400,
		}
	}

	address := request.Get("address").(string)

	err = request.cluster.AddMember(IDUint64, address)

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
