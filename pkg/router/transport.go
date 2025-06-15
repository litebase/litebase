package router

import (
	"io"
	"net/http"
)

type targetKey struct{}

type customTransport struct {
	transport http.Transport
}

type ResponseCloser struct {
	body    io.ReadCloser
	onClose func()
}

func (rc ResponseCloser) Close() error {
	// defer rc.onClose()

	return rc.body.Close()
}

func (rc ResponseCloser) Read(p []byte) (n int, err error) {
	return rc.body.Read(p)
}

func (t *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Retrieve the target from the request's context.
	// target := req.Context().Value(targetKey{}).(*Target)

	// Decrease the connection count when the response body is closed.
	resp.Body = io.ReadCloser(&ResponseCloser{
		body: resp.Body,
		onClose: func() {
			// target.Requests--
		},
	})

	return resp, nil
}
