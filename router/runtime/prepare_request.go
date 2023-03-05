package runtime

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	_auth "litebasedb/internal/auth"
	"litebasedb/internal/config"
	"litebasedb/router/auth"
	"strings"
)

/*
Capture the incoming request from the Router Node that needs to be
forwarded to the Data Runtime. Use the Access Key `sever_sercret_access_key`
to sign the request that is forwarded to the data runtime. When the request
is an internal request used for administration, sign the request differently.
*/
func PrepareRequest(request *RuntimeRequestObject, internal bool) ([]byte, error) {
	queryString := ""

	if len(strings.Split(request.Path, "?")) > 1 {
		queryString = strings.Split(request.Path, "?")[1]
	}

	challenge, err := auth.SecretsManager().EncryptForRuntime(
		request.DatabaseUuid,
		config.Get("signature"),
		request.Headers["x-lbdb-date"],
	)

	if err != nil {
		return nil, err
	}

	server := map[string]interface{}{
		"REQUEST_METHOD":        request.Method,
		"REQUEST_URI":           request.Path,
		"QUERY_STRING":          queryString,
		"HTTP_CONTENT_TYPE":     request.Headers["content-type"],
		"HTTP_HOST":             request.Headers["host"],
		"HTTP_AUTHORIZATION":    request.Headers["authorization"],
		"HTTP_X_LBDB_DATE":      request.Headers["x-lbdb-date"],
		"HTTP_X_LBDB_SIGNATURE": _auth.SignatureHash(config.Get("signature")),
		"HTTP_X_LBDB_CHALLENGE": challenge,
	}

	connectionKey, err := auth.SecretsManager().GetConnectionKey(request.DatabaseUuid, request.BranchUuid)

	if err != nil {
		return nil, err
	}

	if internal {
		accessKeyId := string(sha1.New().Sum([]byte(config.Get("host"))))
		hash := sha256.New()
		hash.Write([]byte(strings.Join([]string{connectionKey, config.Get("region"), config.Get("host"), config.Get("env")}, ":")))
		accessKeySecret := string(hash.Sum(nil))

		server["HTTP_AUTHORIZATION"] = auth.SignRequest(
			accessKeyId,
			accessKeySecret,
			request.Method,
			request.Path,
			request.Headers,
			request.Body,
			request.Query,
		)
	} else {
		serverAccessKeySecret, err := auth.SecretsManager().GetServerSecret(request.DatabaseUuid, request.AccessKeyId)

		if err != nil {
			return nil, err
		}

		server["HTTP_SERVER_AUTHORIZATION"] = auth.SignRequest(
			request.AccessKeyId,
			serverAccessKeySecret,
			request.Method,
			request.Path,
			request.Headers,
			request.Body,
			request.Query,
		)
	}

	preparedRequest := &RuntimeRequest{
		BranchUuid:   request.BranchUuid,
		DatabaseUuid: request.DatabaseUuid,
		Body:         request.Body,
		Method:       request.Method,
		Path:         request.Path,
		Server:       server,
	}

	jsonPayload, err := json.Marshal(preparedRequest)

	if err != nil {
		return nil, err
	}

	return jsonPayload, nil
}
