package messages

type ReplicationGroupAssignment struct {
	Address string
	Role    string
}

type ReplicationGroupAssignmentMessage struct {
	ID          []byte
	Assignments [][]ReplicationGroupAssignment
}
