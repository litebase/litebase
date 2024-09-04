package storage

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"litebase/internal/config"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type S3Client struct {
	accessKeyId     string
	bucket          string
	buffers         sync.Pool
	endpoint        string
	httpClient      http.Client
	region          string
	secretAccessKey string
	signer          *v4.Signer
}

func NewS3Client(bucket string, region string) *S3Client {
	client := &S3Client{
		accessKeyId: os.Getenv("LITEBASE_STORAGE_ACCESS_KEY_ID"),
		bucket:      bucket,
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 256))
			},
		},
		endpoint:        config.Get().StorageEndpoint,
		region:          region,
		secretAccessKey: os.Getenv("LITEBASE_STORAGE_SECRET_ACCESS_KEY"),
		signer:          v4.NewSigner(),
	}

	client.httpClient = http.Client{}

	return client
}

func (s3 *S3Client) createCanonicalRequest(buffer *bytes.Buffer, method, uri, query, payloadHash string, headers map[string]string) []byte {
	buffer.Reset()

	buffer.WriteString(method)
	buffer.WriteString("\n")
	buffer.WriteString(uri)
	buffer.WriteString("\n")
	buffer.WriteString(query)
	buffer.WriteString("\n")

	keys := make([]string, 0, len(headers))

	for k := range headers {
		keys = append(keys, strings.ToLower(k))
	}

	sort.Strings(keys)

	for _, k := range keys {
		buffer.WriteString(k)
		buffer.WriteString(":")
		buffer.WriteString(strings.TrimSpace(headers[k]))
		buffer.WriteString("\n")
	}

	buffer.WriteString("\n")

	for i, k := range keys {
		buffer.WriteString(k)

		if i != len(keys)-1 {
			buffer.WriteString(";")
		}
	}

	buffer.WriteString("\n")

	buffer.WriteString(payloadHash)

	return buffer.Bytes()
}

func (s3 *S3Client) createStringToSign(buffer *bytes.Buffer, timestamp, region, service string, canonicalRequest []byte) []byte {
	buffer.Reset()

	buffer.WriteString("AWS4-HMAC-SHA256\n")
	buffer.WriteString(timestamp)
	buffer.WriteString("\n")
	buffer.WriteString(timestamp[:8])
	buffer.WriteString("/")
	buffer.WriteString(region)
	buffer.WriteString("/")
	buffer.WriteString(service)
	buffer.WriteString("/aws4_request\n")

	h := sha256.New()
	h.Write(canonicalRequest)
	buffer.WriteString(hex.EncodeToString(h.Sum(nil)))

	return buffer.Bytes()
}

func (s3 *S3Client) calculateSignature(secretKey, date, region, service string, stringToSign []byte) string {
	h := hmac.New(sha256.New, []byte("AWS4"+secretKey))
	h.Write([]byte(date))
	dateKey := h.Sum(nil)

	h = hmac.New(sha256.New, dateKey)
	h.Write([]byte(region))
	regionKey := h.Sum(nil)

	h = hmac.New(sha256.New, regionKey)
	h.Write([]byte(service))
	serviceKey := h.Sum(nil)

	h = hmac.New(sha256.New, serviceKey)
	h.Write([]byte("aws4_request"))
	signingKey := h.Sum(nil)

	h = hmac.New(sha256.New, signingKey)
	h.Write(stringToSign)

	return hex.EncodeToString(h.Sum(nil))
}

func (s3 *S3Client) addAuthHeader(headers map[string]string, accessKey, signature, timestamp, region, service string) {
	buffer := s3.buffers.Get().(*bytes.Buffer)
	defer s3.buffers.Put(buffer)

	buffer.Reset()

	buffer.WriteString("AWS4-HMAC-SHA256 Credential=")

	// accessKey/20210810/region/service/aws4_request
	buffer.WriteString(accessKey)
	buffer.WriteString("/")
	buffer.WriteString(timestamp[:8])
	buffer.WriteString("/")
	buffer.WriteString(region)
	buffer.WriteString("/")
	buffer.WriteString(service)
	buffer.WriteString("/aws4_request")

	buffer.WriteString(", SignedHeaders=")
	buffer.WriteString("host;x-amz-content-sha256;x-amz-date")

	buffer.WriteString(", Signature=")
	buffer.WriteString(signature)

	headers["Authorization"] = buffer.String()
}

func (s3 *S3Client) signRequest(request *http.Request, data []byte) {
	date := time.Now().UTC().Format("20060102T150405Z")

	payloadHash := "UNSIGNED-PAYLOAD"
	if request.Method == "PUT" || request.Method == "POST" {
		hash := sha256.New()
		hash.Write(data)
		payloadHash = hex.EncodeToString(hash.Sum(nil))
	}

	headers := map[string]string{
		"host":                 request.URL.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           date,
	}

	uri := request.URL.Path
	queryString := request.URL.RawQuery

	canonicalRequestBuffer := s3.buffers.Get().(*bytes.Buffer)
	defer s3.buffers.Put(canonicalRequestBuffer)

	canonicalRequest := s3.createCanonicalRequest(canonicalRequestBuffer, request.Method, uri, queryString, payloadHash, headers)

	stringToSignBuffer := s3.buffers.Get().(*bytes.Buffer)
	defer s3.buffers.Put(stringToSignBuffer)

	stringToSign := s3.createStringToSign(stringToSignBuffer, date, s3.region, "s3", canonicalRequest)
	signature := s3.calculateSignature(s3.secretAccessKey, date[:8], s3.region, "s3", stringToSign)

	s3.addAuthHeader(headers, s3.accessKeyId, signature, date, s3.region, "s3")

	for k, v := range headers {
		request.Header.Set(k, v)
	}
}

func (s3 *S3Client) url(key string) string {
	var builder strings.Builder
	builder.Grow(len(s3.endpoint) + len(s3.bucket) + len(key) + 2) // Preallocate memory

	builder.WriteString(s3.endpoint)
	builder.WriteByte('/')
	builder.WriteString(s3.bucket)
	builder.WriteByte('/')
	builder.WriteString(key)

	return builder.String()

}

func (s3 *S3Client) GetObject(key string) ([]byte, error) {
	request, err := http.NewRequest("GET", s3.url(key)+"?x-id=GetObject", nil)

	if err != nil {
		return nil, err
	}

	s3.signRequest(request, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		// log.Println(resp)
		data, _ := io.ReadAll(resp.Body)

		defer resp.Body.Close()

		log.Println("DATA", string(data))

		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)

	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s3 *S3Client) PutObject(key string, data []byte) error {
	request, err := http.NewRequest("PUT", s3.url(key)+"?x-id=PutObject", bytes.NewReader(data))

	if err != nil {
		return err
	}

	s3.signRequest(request, data)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		log.Println(err)
		return err
	}

	if resp.StatusCode != 200 {
		log.Println(resp)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (s3 *S3Client) DeleteObject(key string) {
	request, err := http.NewRequest("DELETE", s3.url(key), nil)

	if err != nil {
		return
	}

	s3.signRequest(request, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return
	}

	if resp.StatusCode != 204 {
		return
	}
}
func (s3 *S3Client) ListObjectsV2() {}
