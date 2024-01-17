package actions

var RunAutoScalingLocked bool = false

func RunAutoScaling() {
	if RunAutoScalingLocked {
		return
	}

	RunAutoScalingLocked = true

	// activeDatabases := activeDatabases(0)

	// for _, key := range activeDatabases {
	// 	databaseUuid := strings.Split(key, ":")[0]
	// 	branchUuid := strings.Split(key, ":")[1]
	// }

	RunAutoScalingLocked = false
}
