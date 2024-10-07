package http

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"log"
)

func ClusterMemberDestroyController(request *Request) Response {
	queryNodes, storageNodes := cluster.Get().GetMembers(true)

	ipAddress := request.Headers().Get("X-Lbdb-Node")

	decryptedIp, err := auth.SecretsManager().Decrypt(
		config.Get().Signature,
		ipAddress,
	)

	if err != nil {
		log.Println("Unauthorized node connection attempt: ", ipAddress)

		return Response{
			StatusCode: 401,
		}
	}

	nodePresent := false

	for _, node := range queryNodes {
		if node == decryptedIp["value"] {
			nodePresent = true
			break
		}
	}

	for _, node := range storageNodes {
		if node == decryptedIp["value"] {
			nodePresent = true
			break
		}
	}

	if !nodePresent {
		log.Println("Node is not part of the cluster: ", decryptedIp["value"])

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

	if address != decryptedIp["value"] {
		log.Println("Unauthorized node connection attempt: ", decryptedIp["value"])

		return Response{
			StatusCode: 401,
		}
	}

	cluster.Get().RemoveMember(address)

	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}

func ClusterMemberStoreController(request *Request) Response {
	log.Println("ClusterMemberStoreController")
	queryNodes, storageNodes := cluster.Get().GetMembers(false)

	ipAddress := request.Headers().Get("X-Lbdb-Node")

	if ipAddress == "" {
		log.Println("Unauthorized node connection attempt: ", ipAddress)
	}

	decryptedIp, err := auth.SecretsManager().Decrypt(
		config.Get().Signature,
		ipAddress,
	)

	if err != nil {
		log.Println("Unauthorized node connection attempt: ", ipAddress)

		return Response{
			StatusCode: 401,
		}
	}

	nodePresent := false

	for _, node := range queryNodes {
		if node == decryptedIp["value"] {
			nodePresent = true
			break
		}
	}

	for _, node := range storageNodes {
		if node == decryptedIp["value"] {
			nodePresent = true
			break
		}
	}

	if !nodePresent {
		log.Println("Node is not eligible to join the cluster: ", decryptedIp["value"])

		return Response{
			StatusCode: 400,
		}
	}

	group := request.Get("group").(string)
	address := request.Get("address").(string)

	err = cluster.Get().AddMember(group, address)

	if err != nil {
		log.Println("Failed to add member: ", err)

		return Response{
			StatusCode: 500,
		}
	}

	log.Println("Added member to the cluster: ", address)
	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}
