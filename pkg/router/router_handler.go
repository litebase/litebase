package router

import (
	"log"
	"net/http"
)

func RouterHandler() http.HandlerFunc {
	loadBalancer := NewLoadBalancer()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// attempts := GetAttemptsFromContext(r)

		// if attempts > 3 {
		// 	log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		// 	http.Error(w, "Service not available", http.StatusServiceUnavailable)
		// 	return
		// }

		target, err := loadBalancer.NextTarget()

		if err != nil {
			log.Printf("%s(%s) No targets available\n", r.RemoteAddr, r.URL.Path)
			http.Error(w, "Service not available", http.StatusServiceUnavailable)
			return
		}

		target.ServeHTTP(w, r)
	})
}

func GetAttemptsFromContext(r *http.Request) int {
	attempts := r.Context().Value(Attempts{})

	if attempts == nil {
		return 0
	}

	return attempts.(int)
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry{}).(int); ok {
		return retry
	}
	return 0
}
