package http

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

type Client struct {
	Closed  bool
	read    chan string
	reader  *io.PipeReader
	Request *http.Request
	write   chan string
	writer  *io.PipeWriter
}

func NewClient(host string) *Client {
	return &Client{
		Closed: false,
		read:   make(chan string),
		write:  make(chan string),
	}
}

func (c *Client) Close() {
	if c.Closed {
		return
	}

	c.Closed = true
	c.reader.Close()
	c.writer.Close()
	close(c.read)
	close(c.write)
}

func (c *Client) ListenForMessages(response *http.Response, writer *io.PipeWriter) {
	go func() {
		for message := range c.write {
			_, err := writer.Write([]byte(fmt.Sprintf("%s\n", message)))

			if err != nil {
				log.Println(err)
				c.Close()
				break
			}
		}
	}()

	defer response.Body.Close()

	buf := make([]byte, 1024)

	jsonBlock := ""

	for {
		if c.Closed {
			break
		}

		n, err := response.Body.Read(buf)

		if err != nil {
			c.Close()
			break
		}

		jsonBlock += string(buf[:n])

		// If the json block is not complete, continue reading
		// the json ends in a } with a newline
		if len(jsonBlock) < 2 || jsonBlock[len(jsonBlock)-2:] != "}\n" {
			continue
		}

		c.read <- jsonBlock

		jsonBlock = ""
	}
}

/*
Send a request to the router. The request body will be streamed to the router,
and the response body will be streamed back to the client.
*/
func (c *Client) Open(host string, path string, headers map[string][]string) error {
	reader, writer := io.Pipe()
	c.reader = reader
	c.writer = writer

	body := io.NopCloser(reader)

	client := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/%s", host, path), body)
	request.Header.Set("Connection", "keep-alive")

	if err != nil {
		log.Println(err)
		return err
	}

	request.Header = headers

	go func() {
		_, err = writer.Write([]byte("{}\n"))

		if err != nil {
			log.Fatalln(err)
		}
	}()

	response, err := client.Do(request)

	if err != nil {
		log.Println(err)
		return err
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("Connection failed with status code %d", response.StatusCode)
	}

	go c.ListenForMessages(response, writer)

	return nil
}

func (c *Client) Send(message string) {
	c.write <- message
}
