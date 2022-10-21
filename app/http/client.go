package http

import (
	"bufio"
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"net/url"

	"golang.org/x/net/http2"
)

type Client struct {
	client   *http.Client
	End      chan bool
	Messages chan string
}

func NewClient() *Client {
	return &Client{
		End:      make(chan bool),
		Messages: make(chan string),
	}
}

func (c *Client) Dial() {
	// Adds TLS cert-key pair
	certs, err := tls.LoadX509KeyPair("./key.crt", "./key.key")
	if err != nil {
		log.Fatal(err)
	}

	t := &http2.Transport{
		// DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
		// 	cfg.Certificates = []tls.Certificate{certs}
		// 	return tls.Dial(network, addr, cfg)
		// },
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{certs},
			InsecureSkipVerify: true,
		},
	}
	c.client = &http.Client{
		Transport: t,
		Timeout:   0,
	}
}

func (c *Client) Close() {
	c.client.CloseIdleConnections()
}

func (c *Client) ListenForMessages(writer *io.PipeWriter) {
	for {
		select {
		case message := <-c.Messages:
			writer.Write([]byte(message))
		case <-c.End:
			writer.Close()
			return
		}
	}
}

func (c *Client) Open(host string, path string, headers map[string][]string) error {
	reader, writer := io.Pipe()

	request := &http.Request{
		// ContentLength: -1,
		Method: "POST",
		URL: &url.URL{
			Scheme: "https",
			Host:   host,
			Path:   path,
		},
		Header: headers,
		Body:   io.NopCloser(reader),
	}

	go c.ListenForMessages(writer)

	response, err := c.client.Do(request)

	if err != nil {
		log.Println(err)
		return err
	}

	// if response.StatusCode == 500 {
	// 	return
	// }

	defer response.Body.Close()

	bufferedReader := bufio.NewReader(response.Body)

	buffer := make([]byte, 4*1024)

	for {
		len, err := bufferedReader.Read(buffer)

		if len > 0 {
			log.Println(len, "bytes received")
			log.Println(string(buffer[:len]))
		}

		if err != nil {
			if err == io.EOF {
				log.Println(err)
			}
			break
		}
	}

	return nil
}

func (c *Client) Send(message string) {
	c.Messages <- message
}
