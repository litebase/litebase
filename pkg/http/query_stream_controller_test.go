package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func openQueryStreamConnection(
	ctx context.Context,
	cancel context.CancelFunc,
	urlString, method, accessKeyId, accessKeySecret string,
) (input chan []byte, output chan []byte, errChan chan error) {
	errChan = make(chan error, 10)
	input = make(chan []byte, 10)
	output = make(chan []byte, 10)
	reader, writer := io.Pipe()
	connecting := true
	connected := make(chan struct{}, 1)
	connectedDeadline := time.NewTimer(3 * time.Second)

	bufferedWriter := bufio.NewWriter(writer)

	parsedUrl, err := url.Parse(urlString)

	if err != nil {
		errChan <- err
		return
	}

	host := parsedUrl.Hostname()

	if parsedUrl.Port() != "" {
		host = fmt.Sprintf("%s:%s", host, parsedUrl.Port())
	}

	signature := auth.SignRequest(
		accessKeyId,
		accessKeySecret,
		method,
		parsedUrl.Path,
		map[string]string{
			"Content-Length": "0",
			"Content-Type":   "application/octet-stream",
			"Host":           host,
			"X-LBDB-Date":    fmt.Sprintf("%d", time.Now().UTC().Unix()),
		},
		map[string]any{},
		map[string]string{},
	)

	request, err := http.NewRequestWithContext(ctx, method, urlString, reader)

	if err != nil {
		errChan <- err
		return
	}

	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", signature)
	request.Header.Set("X-LBDB-Date", fmt.Sprintf("%d", time.Now().UTC().Unix()))

	client := &http.Client{
		Timeout: 0,
	}

	// Start the connection by sending the connection message
	go func() {
		// Send the message type to the server
		_, err := writer.Write([]byte{0x01})

		if err != nil {
			log.Println("Error writing connection message")
			errChan <- err
			return
		}

		var messageLengthBytes [4]byte
		binary.LittleEndian.PutUint32(messageLengthBytes[:], uint32(0))
		writer.Write(messageLengthBytes[:])

		err = bufferedWriter.Flush()

		if err != nil {
			log.Println("Error flushing connection message")
			errChan <- err
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-connectedDeadline.C:
				errChan <- fmt.Errorf("connection timeout")
				log.Println("Connection timeout")
				return
			case buffer := <-input:
				if connecting {
					<-connected
					connecting = false
				}

				_, err := writer.Write([]byte{0x04})

				if err != nil {
					log.Println("Error writing buffer to writer", err)
					return
				}

				var messageLengthBytes [4]byte
				binary.LittleEndian.PutUint32(messageLengthBytes[:], uint32(len(buffer)))
				_, err = writer.Write(messageLengthBytes[:])

				if err != nil {
					log.Println("Error writing buffer to writer", err)
					return
				}

				_, err = writer.Write(buffer)

				if err != nil {
					log.Println("Error writing buffer to writer", err)
					return
				}

				err = bufferedWriter.Flush()

				if err != nil {
					log.Println("Error flushing buffer to writer", err)
					return
				}
			}
		}
	}()

	// Read and write to the server
	go func() {
		response, err := client.Do(request)

		if err != nil {
			errChan <- err
			return
		}

		if response.StatusCode != 200 {
			errChan <- fmt.Errorf("unexpected status code: %d", response.StatusCode)
			return
		}

		defer response.Body.Close()
		scanBuffer := bytes.NewBuffer(make([]byte, 1024))

		messageHeaderBytes := make([]byte, 5)

		for {
			select {
			case <-ctx.Done():
				return
			case <-connectedDeadline.C:
				errChan <- fmt.Errorf("connection timeout")
				log.Println("Connection timeout")
				return
			default:
				scanBuffer.Reset()

				_, err := response.Body.Read(messageHeaderBytes)

				if err != nil {
					cancel()
					return
				}

				messageType := messageHeaderBytes[0]

				messageLength := int(binary.LittleEndian.Uint32(messageHeaderBytes[1:]))

				bytesRead := 0

				for bytesRead < messageLength {
					chunkSize := 1024 // Define a chunk size

					if messageLength-bytesRead < chunkSize {
						chunkSize = messageLength - bytesRead
					}

					n, err := io.CopyN(scanBuffer, response.Body, int64(chunkSize))

					if err != nil {
						log.Println(err)
						break
					}

					bytesRead += int(n)
				}

				switch messageType {
				case 0x01:
					connectedDeadline.Stop()

					connected <- struct{}{}
				case 0x03:
					errChan <- errors.New(string(scanBuffer.Bytes()[0:messageLength]))
				case 0x04:
					output <- scanBuffer.Bytes()[0:messageLength]
				}
			}
		}
	}()

	return
}

func TestQueryStreamController(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		testCases := []*database.QueryInput{
			{
				Id:         []byte(uuid.NewString()),
				Statement:  []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"),
				Parameters: nil,
			},
			{
				Id:        []byte(uuid.NewString()),
				Statement: []byte("INSERT INTO test (name) VALUES (?)"),
				Parameters: []sqlite3.StatementParameter{
					{
						Type:  "TEXT",
						Value: "name1",
					},
				},
			},
			{
				Id:         []byte(uuid.NewString()),
				Statement:  []byte("SELECT * FROM test"),
				Parameters: nil,
			},
		}

		url := fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseKey.Key,
		)

		ctx, cancel := context.WithCancel(context.Background())

		inputChannel, outputChannel, errorChannel := openQueryStreamConnection(
			ctx,
			cancel,
			url,
			"POST",
			testDatabase.AccessKey.AccessKeyId,
			testDatabase.AccessKey.AccessKeySecret,
		)

		statementBuffer := bytes.NewBuffer(make([]byte, 1024))
		queryStreamFrameBuffer := bytes.NewBuffer(make([]byte, 1024))

		for _, testCase := range testCases {
			statementBuffer.Reset()

			testCase.Encode(statementBuffer)

			queryStreamFrameBuffer.Reset()

			var statementLengthBytes [4]byte
			binary.LittleEndian.PutUint32(statementLengthBytes[:], uint32(statementBuffer.Len()))
			queryStreamFrameBuffer.Write(statementLengthBytes[:])

			queryStreamFrameBuffer.Write(statementBuffer.Bytes())

			inputChannel <- queryStreamFrameBuffer.Bytes()

			// Send
		responseLoop:
			for {
				select {
				case <-ctx.Done():
					log.Println("Context done")
					return
				case <-outputChannel:
					break responseLoop
				case err := <-errorChannel:
					t.Fatal(err)
					return
				}
			}
		}

		// Close the connection
		cancel()
	})
}
