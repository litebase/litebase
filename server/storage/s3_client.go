package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"litebase/internal/config"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type S3Client struct {
	accessKeyId     string
	bucket          string
	buffers         sync.Pool
	context         context.Context
	Endpoint        string
	httpClient      http.Client
	region          string
	secretAccessKey string
}

type Delete struct {
	XMLName xml.Name           `xml:"Delete"`
	Objects []ObjectIdentifier `xml:"Object"`
	Quiet   bool               `xml:"Quiet"`
}

type ObjectIdentifier struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

func NewS3Client(c *config.Config, bucket string, region string) *S3Client {
	client := &S3Client{
		accessKeyId: c.StorageAccessKeyId,
		bucket:      bucket,
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 256))
			},
		},
		context:         context.Background(),
		Endpoint:        c.StorageEndpoint,
		region:          region,
		secretAccessKey: c.StorageSecretAccessKey,
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

	// Transform the raw query to UriEncode(<QueryParameter>) + "=" + UriEncode(<value>) + "&"
	parsedQuery, err := url.ParseQuery(query)

	if err != nil {
		log.Println(err)
		return nil
	}

	// Sort the query parameters by key
	var keys []string

	for key := range parsedQuery {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	// Reconstruct the query string with URI-encoded components
	var encodedQuery []string

	for _, key := range keys {
		for _, value := range parsedQuery[key] {
			encodedQuery = append(encodedQuery, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}

	buffer.WriteString(strings.Join(encodedQuery, "&"))

	buffer.WriteString("\n")

	keys = make([]string, 0, len(headers))

	for k := range headers {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		buffer.WriteString(strings.ToLower(k))
		buffer.WriteString(":")
		buffer.WriteString(strings.TrimSpace(headers[k]))
		buffer.WriteString("\n")
	}

	buffer.WriteString("\n")

	for i, k := range keys {
		buffer.WriteString(strings.ToLower(k))

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

	keys := make([]string, 0, len(headers))

	for k := range headers {
		keys = append(keys, strings.ToLower(k))
	}

	sort.Strings(keys)

	for i, k := range keys {
		buffer.WriteString(k)

		if i != len(keys)-1 {
			buffer.WriteString(";")
		}
	}

	buffer.WriteString(", Signature=")
	buffer.WriteString(signature)

	headers["Authorization"] = buffer.String()
}

func (s3 *S3Client) signRequest(request *http.Request, data []byte, additionalHeaders map[string]string) {
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

	for k, v := range additionalHeaders {
		headers[k] = v
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
	builder.Grow(len(s3.Endpoint) + len(s3.bucket) + len(key) + 2) // Preallocate memory

	builder.WriteString(s3.Endpoint)
	builder.WriteByte('/')
	builder.WriteString(s3.bucket)

	if key != "" {
		builder.WriteByte('/')
		builder.WriteString(key)
	}

	return builder.String()
}

func (s3 *S3Client) CopyObject(sourceKey, destinationKey string) error {
	request, err := http.NewRequestWithContext(s3.context, "PUT", s3.url(destinationKey), nil)

	if err != nil {
		return err
	}

	request.Header.Set("x-amz-copy-source", s3.bucket+"/"+sourceKey)

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

type CreateBucketResponse struct {
	StatusCode int
}

func (s3 *S3Client) CreateBucket() (CreateBucketResponse, error) {
	request, err := http.NewRequestWithContext(s3.context, "PUT", s3.url(""), nil)

	if err != nil {
		return CreateBucketResponse{}, err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return CreateBucketResponse{}, err
	}

	if resp.StatusCode != 200 {
		return CreateBucketResponse{
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return CreateBucketResponse{
		StatusCode: resp.StatusCode,
	}, nil
}

func (s3 *S3Client) DeleteObject(key string) error {
	request, err := http.NewRequestWithContext(s3.context, "DELETE", s3.url(key), nil)

	if err != nil {
		return err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return err
	}

	if resp.StatusCode != 204 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (s3 *S3Client) DeleteObjects(keys []string) error {
	objectsToDelete := make([]ObjectIdentifier, len(keys))

	for i, key := range keys {
		objectsToDelete[i] = ObjectIdentifier{
			Key: key,
		}
	}

	data, err := xml.Marshal(Delete{
		Objects: objectsToDelete,
		Quiet:   true,
	})

	if err != nil {
		return err
	}

	// Compute the MD5 hash of the request body
	hash := md5.New()
	hash.Write(data)
	md5Sum := hash.Sum(nil)

	request, err := http.NewRequestWithContext(s3.context, "POST", s3.url("")+"?delete", bytes.NewReader(data))

	if err != nil {
		return err
	}

	s3.signRequest(request, data, map[string]string{
		"Content-Md5": base64.StdEncoding.EncodeToString(md5Sum),
	})

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

type GetObjectResponse struct {
	Data       []byte
	StatusCode int
}

func (s3 *S3Client) GetObject(key string) (GetObjectResponse, error) {
	request, err := http.NewRequestWithContext(s3.context, "GET", s3.url(key)+"?x-id=GetObject", nil)

	if err != nil {
		return GetObjectResponse{}, err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return GetObjectResponse{}, err
	}

	if resp.StatusCode != 200 {
		return GetObjectResponse{
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)

	defer resp.Body.Close()

	if err != nil {
		return GetObjectResponse{}, err
	}

	return GetObjectResponse{
		Data:       data,
		StatusCode: resp.StatusCode,
	}, nil
}

type HeadBucketResponse struct {
	StatusCode int
}

func (s3 *S3Client) HeadBucket() (HeadBucketResponse, error) {
	request, err := http.NewRequestWithContext(s3.context, "HEAD", s3.url(""), nil)

	if err != nil {
		return HeadBucketResponse{}, err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return HeadBucketResponse{}, err
	}

	if resp.StatusCode != 200 {
		return HeadBucketResponse{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return HeadBucketResponse{
			StatusCode: resp.StatusCode,
		},
		nil
}

type HeadObjectResponse struct {
	ChecksumSHA256 string
	ContentLength  int64
	ContentType    string
	Etag           string
	LastModified   time.Time
	StatusCode     int
}

func (s3 *S3Client) HeadObject(key string) (HeadObjectResponse, error) {
	request, err := http.NewRequestWithContext(s3.context, "HEAD", s3.url(key), nil)

	if err != nil {
		return HeadObjectResponse{}, err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return HeadObjectResponse{}, err
	}

	if resp.StatusCode != 200 {
		return HeadObjectResponse{
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	lastModified, _ := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))

	return HeadObjectResponse{
		ContentLength: contentLength,
		ContentType:   resp.Header.Get("Content-Type"),
		Etag:          resp.Header.Get("Etag"),
		LastModified:  lastModified,
		StatusCode:    resp.StatusCode,
	}, nil
}

func (s3 *S3Client) ListObjectsV2(input ListObjectsV2Input) (ListObjectsV2Response, error) {
	url := s3.url("") + "?list-type=2"

	if input.MaxKeys != 0 {
		url += "&max-keys=" + strconv.Itoa(input.MaxKeys)
	}

	if input.Prefix != "" {
		url += "&prefix=" + input.Prefix

		if input.Delimiter != "" {
			url += "&delimiter=" + input.Delimiter
		}
	} else {
		url += "&prefix=/"
	}

	if input.ContinuationToken != "" {
		url += "&continuation-token=" + input.ContinuationToken
	}

	if input.StartAfter != "" {
		url += "&start-after=" + input.StartAfter
	}

	request, err := http.NewRequestWithContext(s3.context, "GET", url, nil)

	if err != nil {
		return ListObjectsV2Response{}, err
	}

	s3.signRequest(request, nil, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return ListObjectsV2Response{}, err
	}

	if resp.StatusCode != 200 {
		return ListObjectsV2Response{
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Unmarshal the XML response
	var listBucketResult ListBucketResult

	decoder := xml.NewDecoder(resp.Body)

	err = decoder.Decode(&listBucketResult)

	if err != nil {
		return ListObjectsV2Response{
			StatusCode: resp.StatusCode,
		}, err
	}

	return ListObjectsV2Response{
		ListBucketResult: listBucketResult,
		StatusCode:       resp.StatusCode,
	}, nil
}

type PutObjectResponse struct {
	StatusCode int
}

func (s3 *S3Client) PutObject(key string, data []byte) (PutObjectResponse, error) {
	request, err := http.NewRequestWithContext(s3.context, "PUT", s3.url(key)+"?x-id=PutObject", bytes.NewReader(data))

	if err != nil {
		return PutObjectResponse{}, err
	}

	s3.signRequest(request, data, nil)

	resp, err := s3.httpClient.Do(request)

	if err != nil {
		return PutObjectResponse{}, err
	}

	if resp.StatusCode != 200 {
		return PutObjectResponse{
			StatusCode: resp.StatusCode,
		}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return PutObjectResponse{
		StatusCode: resp.StatusCode,
	}, nil
}
