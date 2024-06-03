package router

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

type Attempts struct{}
type Retry struct{}

type Target struct {
	alive          int32
	RevereseProxy  *httputil.ReverseProxy
	transportIndex int64
	transports     []*customTransport
	URL            *url.URL
}

func NewTarget(lb *LoadBalancer, host, port string) *Target {
	url, err := url.Parse(fmt.Sprintf("http://%s:%s", host, port))

	if err != nil {
		panic(err)
	}

	target := &Target{
		alive: 1,
		URL:   url,
	}

	proxy := httputil.NewSingleHostReverseProxy(url)

	// Disable the default logging
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Ignore context canceled errors
		if err.Error() == "context canceled" {
			return
		}

		log.Printf("%s\n", err.Error())
	}

	target.RevereseProxy = proxy

	// Create the transports
	for i := 0; i < 100; i++ {
		transport := &customTransport{http.Transport{
			// DialContext: (&net.Dialer{
			// 	Timeout:   30 * time.Second,
			// 	KeepAlive: 30 * time.Second,
			// }).DialContext,
			MaxIdleConns:    0,
			MaxConnsPerHost: 0,
			IdleConnTimeout: 30 * time.Second,
		}}
		target.transports = append(target.transports, transport)
	}

	// proxy.Transport = &customTransport{http.Transport{
	// 	// DialContext: (&net.Dialer{
	// 	// 	Timeout:   30 * time.Second,
	// 	// 	KeepAlive: 30 * time.Second,
	// 	// }).DialContext,
	// 	MaxIdleConns:    100000,
	// 	MaxConnsPerHost: 1000,
	// 	IdleConnTimeout: 1 * time.Second,
	// }}

	return target
}

func (target *Target) IsAlive() bool {
	return atomic.LoadInt32(&target.alive) == 1
}

func (target *Target) SetAlive(alive bool) {
	var i int32

	if alive {
		i = 1
	}

	atomic.StoreInt32(&target.alive, i)
}

func (target *Target) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target.transportIndex = (target.transportIndex + 1) % int64(len(target.transports))
	target.RevereseProxy.Transport = target.transports[target.transportIndex]
	target.RevereseProxy.ServeHTTP(w, r)
}
