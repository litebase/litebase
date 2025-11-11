package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
)

// ChunkedSignatureValidator validates chunk signatures in LQTP streaming requests.
// It implements chunked signature validation similar to AWS Signature Version 4,
// where each chunk's signature chains from the previous signature to ensure
// data integrity, authenticity, and correct ordering of chunks.
type ChunkedSignatureValidator struct {
	accessKeySecret   string
	date              string
	previousSignature string
}

// NewChunkedSignatureValidator creates a new validator with the seed signature.
// The seed signature is extracted from the initial HTTP request's Authorization header.
// This signature serves as the starting point for the signature chain.
func NewChunkedSignatureValidator(accessKeySecret, date, seedSignature string) *ChunkedSignatureValidator {
	return &ChunkedSignatureValidator{
		accessKeySecret:   accessKeySecret,
		date:              date,
		previousSignature: seedSignature,
	}
}

// ValidateChunk validates a chunk's signature and updates the previous signature.
// This implements the LQTP chunked signature validation protocol:
//  1. Calculate chunk hash: chunkHash = SHA256(chunkData)
//  2. Create string to sign: stringToSign = previousSignature + chunkHash
//  3. Generate signing key chain:
//     - dateKey = HMAC-SHA256(accessKeySecret, date)
//     - serviceKey = HMAC-SHA256(dateKey, "litebase_request")
//  4. Calculate expected signature: HMAC-SHA256(serviceKey, stringToSign)
//  5. Compare with provided signature using constant-time comparison
//  6. Update previousSignature for the next chunk
//
// Parameters:
//   - chunkData: The raw chunk data (frame data without signature metadata)
//   - chunkSignature: The hex-encoded signature sent with the chunk
//
// Returns an error if validation fails, nil otherwise.
func (v *ChunkedSignatureValidator) ValidateChunk(chunkData []byte, chunkSignature string) error {
	// Calculate the hash of the chunk data
	chunkHashSum := sha256.Sum256(chunkData)
	chunkHash := fmt.Sprintf("%x", chunkHashSum)

	// Create the string to sign for this chunk
	// Format: previousSignature + chunkHash
	stringToSign := v.previousSignature + chunkHash

	// Create the signing key chain (same as in request signature validation)
	dateHash := hmac.New(sha256.New, []byte(v.accessKeySecret))
	dateHash.Write([]byte(v.date))
	dateKey := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(dateKey))
	serviceHash.Write([]byte("litebase_request"))
	serviceKey := fmt.Sprintf("%x", serviceHash.Sum(nil))

	// Calculate the expected signature
	signatureHash := hmac.New(sha256.New, []byte(serviceKey))
	signatureHash.Write([]byte(stringToSign))
	expectedSignature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	// Compare signatures using constant time comparison to prevent timing attacks
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(chunkSignature)) != 1 {
		return fmt.Errorf("chunk signature validation failed: expected %s, got %s", expectedSignature, chunkSignature)
	}

	// Update the previous signature for the next chunk
	v.previousSignature = chunkSignature

	return nil
}

// GetPreviousSignature returns the current previous signature (for testing/debugging)
func (v *ChunkedSignatureValidator) GetPreviousSignature() string {
	return v.previousSignature
}
