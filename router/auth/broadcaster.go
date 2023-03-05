package auth

var broadcaster func(key string, value string)

func Broadcast(key string, value string) {
	if broadcaster != nil {
		broadcaster(key, value)
	}
}

func Broadcaster(f func(key string, value string)) {
	broadcaster = f
}
