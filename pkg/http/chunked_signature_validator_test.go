package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"testing"
)

// Helper function to create a chunk signature (same logic as client-side SignChunk)
func signChunkForTest(accessKeySecret, date, previousSignature string, chunkData []byte) string {
	chunkHashSum := sha256.Sum256(chunkData)
	chunkHash := fmt.Sprintf("%x", chunkHashSum)

	stringToSign := previousSignature + chunkHash

	dateHash := hmac.New(sha256.New, []byte(accessKeySecret))
	dateHash.Write([]byte(date))
	dateKey := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(dateKey))
	serviceHash.Write([]byte("litebase_request"))
	serviceKey := fmt.Sprintf("%x", serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(serviceKey))
	signatureHash.Write([]byte(stringToSign))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	return signature
}

func TestNewChunkedSignatureValidator(t *testing.T) {
	accessKeySecret := "test-secret-key"
	date := "1699718400"
	seedSignature := "abc123def456"

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	if validator == nil {
		t.Fatal("Expected validator to be non-nil")
	}

	if validator.accessKeySecret != accessKeySecret {
		t.Errorf("Expected accessKeySecret %s, got %s", accessKeySecret, validator.accessKeySecret)
	}

	if validator.date != date {
		t.Errorf("Expected date %s, got %s", date, validator.date)
	}

	if validator.previousSignature != seedSignature {
		t.Errorf("Expected previousSignature %s, got %s", seedSignature, validator.previousSignature)
	}
}

func TestValidateChunk_ValidSignature(t *testing.T) {
	accessKeySecret := "my-secret-key-12345"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	chunkData := []byte("test chunk data")

	// Create validator with seed signature
	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// Generate a valid signature for the chunk
	validSignature := signChunkForTest(accessKeySecret, date, seedSignature, chunkData)

	// Validate the chunk
	err := validator.ValidateChunk(chunkData, validSignature)

	if err != nil {
		t.Errorf("Expected validation to succeed, got error: %v", err)
	}

	// Verify the previous signature was updated
	if validator.GetPreviousSignature() != validSignature {
		t.Errorf("Expected previousSignature to be updated to %s, got %s", validSignature, validator.GetPreviousSignature())
	}
}

func TestValidateChunk_InvalidSignature(t *testing.T) {
	accessKeySecret := "my-secret-key-12345"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	chunkData := []byte("test chunk data")

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// Use an invalid signature
	invalidSignature := "this-is-not-a-valid-signature"

	err := validator.ValidateChunk(chunkData, invalidSignature)

	if err == nil {
		t.Error("Expected validation to fail with invalid signature, but it succeeded")
	}

	// Verify the previous signature was NOT updated
	if validator.GetPreviousSignature() != seedSignature {
		t.Errorf("Expected previousSignature to remain %s after failed validation, got %s", seedSignature, validator.GetPreviousSignature())
	}
}

func TestValidateChunk_SignatureChaining(t *testing.T) {
	accessKeySecret := "my-secret-key-12345"
	date := "1699718400"
	seedSignature := "initial-seed-signature"

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// First chunk
	chunk1Data := []byte("first chunk")
	chunk1Signature := signChunkForTest(accessKeySecret, date, seedSignature, chunk1Data)

	err := validator.ValidateChunk(chunk1Data, chunk1Signature)
	if err != nil {
		t.Fatalf("First chunk validation failed: %v", err)
	}

	// Second chunk - must use the signature from the first chunk
	chunk2Data := []byte("second chunk")
	chunk2Signature := signChunkForTest(accessKeySecret, date, chunk1Signature, chunk2Data)

	err = validator.ValidateChunk(chunk2Data, chunk2Signature)

	if err != nil {
		t.Fatalf("Second chunk validation failed: %v", err)
	}

	// Third chunk - must use the signature from the second chunk
	chunk3Data := []byte("third chunk")
	chunk3Signature := signChunkForTest(accessKeySecret, date, chunk2Signature, chunk3Data)

	err = validator.ValidateChunk(chunk3Data, chunk3Signature)

	if err != nil {
		t.Fatalf("Third chunk validation failed: %v", err)
	}

	// Verify final signature
	if validator.GetPreviousSignature() != chunk3Signature {
		t.Errorf("Expected final previousSignature to be %s, got %s", chunk3Signature, validator.GetPreviousSignature())
	}
}

func TestValidateChunk_WrongOrderFails(t *testing.T) {
	accessKeySecret := "my-secret-key-12345"
	date := "1699718400"
	seedSignature := "initial-seed-signature"

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// First chunk
	chunk1Data := []byte("first chunk")
	chunk1Signature := signChunkForTest(accessKeySecret, date, seedSignature, chunk1Data)

	err := validator.ValidateChunk(chunk1Data, chunk1Signature)

	if err != nil {
		t.Fatalf("First chunk validation failed: %v", err)
	}

	// Second chunk
	chunk2Data := []byte("second chunk")
	chunk2Signature := signChunkForTest(accessKeySecret, date, chunk1Signature, chunk2Data)

	err = validator.ValidateChunk(chunk2Data, chunk2Signature)

	if err != nil {
		t.Fatalf("Second chunk validation failed: %v", err)
	}

	// Try to send chunk1 again - should fail because it's using the old signature
	err = validator.ValidateChunk(chunk1Data, chunk1Signature)

	if err == nil {
		t.Error("Expected validation to fail when replaying a chunk, but it succeeded")
	}
}

func TestValidateChunk_DifferentSecretFails(t *testing.T) {
	accessKeySecret := "correct-secret"
	wrongSecret := "wrong-secret"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	chunkData := []byte("test chunk")

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// Generate signature with wrong secret
	wrongSignature := signChunkForTest(wrongSecret, date, seedSignature, chunkData)

	err := validator.ValidateChunk(chunkData, wrongSignature)

	if err == nil {
		t.Error("Expected validation to fail with wrong secret, but it succeeded")
	}
}

func TestValidateChunk_DifferentDateFails(t *testing.T) {
	accessKeySecret := "my-secret-key"
	correctDate := "1699718400"
	wrongDate := "1699718401"
	seedSignature := "initial-seed-signature"
	chunkData := []byte("test chunk")

	validator := NewChunkedSignatureValidator(accessKeySecret, correctDate, seedSignature)

	// Generate signature with wrong date
	wrongSignature := signChunkForTest(accessKeySecret, wrongDate, seedSignature, chunkData)

	err := validator.ValidateChunk(chunkData, wrongSignature)

	if err == nil {
		t.Error("Expected validation to fail with wrong date, but it succeeded")
	}
}

func TestValidateChunk_EmptyChunkData(t *testing.T) {
	accessKeySecret := "my-secret-key"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	chunkData := []byte{}

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	validSignature := signChunkForTest(accessKeySecret, date, seedSignature, chunkData)

	err := validator.ValidateChunk(chunkData, validSignature)

	if err != nil {
		t.Errorf("Expected validation to succeed for empty chunk, got error: %v", err)
	}
}

func TestValidateChunk_LargeChunkData(t *testing.T) {
	accessKeySecret := "my-secret-key"
	date := "1699718400"
	seedSignature := "initial-seed-signature"

	// Create a large chunk (1MB)
	chunkData := make([]byte, 1024*1024)

	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	validSignature := signChunkForTest(accessKeySecret, date, seedSignature, chunkData)

	err := validator.ValidateChunk(chunkData, validSignature)

	if err != nil {
		t.Errorf("Expected validation to succeed for large chunk, got error: %v", err)
	}
}

func TestValidateChunk_ModifiedChunkDataFails(t *testing.T) {
	accessKeySecret := "my-secret-key"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	originalData := []byte("original chunk data")

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	// Generate signature for original data
	signature := signChunkForTest(accessKeySecret, date, seedSignature, originalData)

	// Modify the data
	modifiedData := []byte("modified chunk data")

	// Validation should fail because data doesn't match signature
	err := validator.ValidateChunk(modifiedData, signature)

	if err == nil {
		t.Error("Expected validation to fail for modified chunk data, but it succeeded")
	}
}

func TestGetPreviousSignature(t *testing.T) {
	seedSignature := "test-seed-signature"
	validator := NewChunkedSignatureValidator("secret", "1699718400", seedSignature)

	if validator.GetPreviousSignature() != seedSignature {
		t.Errorf("Expected initial previousSignature %s, got %s", seedSignature, validator.GetPreviousSignature())
	}

	// After successful validation, previous signature should update
	chunkData := []byte("test data")
	newSignature := signChunkForTest("secret", "1699718400", seedSignature, chunkData)

	validator.ValidateChunk(chunkData, newSignature)

	if validator.GetPreviousSignature() != newSignature {
		t.Errorf("Expected previousSignature to update to %s, got %s", newSignature, validator.GetPreviousSignature())
	}
}

func TestValidateChunk_SignatureFormatMattersPerByte(t *testing.T) {
	accessKeySecret := "my-secret-key"
	date := "1699718400"
	seedSignature := "initial-seed-signature"
	chunkData := []byte("test chunk")

	validator := NewChunkedSignatureValidator(accessKeySecret, date, seedSignature)

	validSignature := signChunkForTest(accessKeySecret, date, seedSignature, chunkData)

	// Change one character in the signature
	if len(validSignature) > 0 {
		invalidSignature := validSignature[:len(validSignature)-1] + "x"

		err := validator.ValidateChunk(chunkData, invalidSignature)

		if err == nil {
			t.Error("Expected validation to fail with single character change in signature")
		}
	}
}
