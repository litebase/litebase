package storage

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptedStreamFile_NewAndWriteHeader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header
	err = esf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Verify header was written
	headerBuf := make([]byte, StreamHeaderSize)
	n, err := file.ReadAt(headerBuf, 0)

	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if n != StreamHeaderSize {
		t.Errorf("expected header size %d, got %d", StreamHeaderSize, n)
	}

	// Verify header content
	header, err := ReadStreamHeader(bytes.NewReader(headerBuf))

	if err != nil {
		t.Fatalf("failed to parse header: %v", err)
	}

	if !bytes.Equal(header.KeyHash[:], keyHash[:]) {
		t.Error("key hash mismatch in header")
	}
}

func TestEncryptedStreamFile_OpenAndVerifyHeader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create and write header
	{
		file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

		if err != nil {
			t.Fatalf("NewEncryptedStreamFile failed: %v", err)
		}

		err = esf.WriteHeader()

		if err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close test file: %v", err)
		}
	}

	// Open and verify header
	{
		file, err := os.OpenFile(testFile, os.O_RDWR, 0600)

		if err != nil {
			t.Fatalf("failed to open test file: %v", err)
		}

		defer func() {
			if err := file.Close(); err != nil {
				t.Errorf("failed to close test file: %v", err)
			}
		}()

		esf, err := OpenEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

		if err != nil {
			t.Fatalf("OpenEncryptedStreamFile failed: %v", err)
		}

		if esf == nil {
			t.Fatal("expected non-nil EncryptedStreamFile")
		}
	}
}

func TestEncryptedStreamFile_OpenWithWrongKey(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create and write header with original key
	{
		file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

		if err != nil {
			t.Fatalf("NewEncryptedStreamFile failed: %v", err)
		}

		err = esf.WriteHeader()

		if err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close test file: %v", err)
		}
	}

	// Try to open with different key
	{
		file, err := os.OpenFile(testFile, os.O_RDWR, 0600)

		if err != nil {
			t.Fatalf("failed to open test file: %v", err)
		}

		defer func() {
			if err := file.Close(); err != nil {
				t.Errorf("failed to close test file: %v", err)
			}
		}()

		wrongKey := make([]byte, 32)

		for i := range wrongKey {
			wrongKey[i] = byte(i + 1) // Different key
		}

		wrongKeyHash := sha256.Sum256(wrongKey)

		_, err = OpenEncryptedStreamFile(file, wrongKey, wrongKeyHash, 123456, "/test.wal")

		if err == nil {
			t.Error("expected error when opening with wrong key, got nil")
		}
	}
}

func TestEncryptedStreamFile_WriteAtReadAt_RoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header
	err = esf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Create test data (4096 bytes)
	pageData := make([]byte, StreamPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write encrypted page at offset 0
	n, err := esf.WriteAt(pageData, 0)

	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != StreamPageSize {
		t.Errorf("expected to write %d bytes, wrote %d", StreamPageSize, n)
	}

	// Read encrypted page back
	readBuf := make([]byte, StreamPageSize)
	n, err = esf.ReadAt(readBuf, 0)

	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if n != StreamPageSize {
		t.Errorf("expected to read %d bytes, read %d", StreamPageSize, n)
	}

	// Verify round-trip
	if !bytes.Equal(readBuf, pageData) {
		t.Error("read data does not match written data")
	}
}

func TestEncryptedStreamFile_MultiplePages(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header
	err = esf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write multiple pages
	numPages := 5

	for pageNum := 0; pageNum < numPages; pageNum++ {
		pageData := make([]byte, StreamPageSize)

		for i := range pageData {
			pageData[i] = byte((i + pageNum) % 256)
		}

		offset := int64(pageNum * StreamPageSize)
		_, err := esf.WriteAt(pageData, offset)

		if err != nil {
			t.Fatalf("WriteAt failed for page %d: %v", pageNum, err)
		}
	}

	// Read and verify all pages
	for pageNum := 0; pageNum < numPages; pageNum++ {
		expectedData := make([]byte, StreamPageSize)

		for i := range expectedData {
			expectedData[i] = byte((i + pageNum) % 256)
		}

		readBuf := make([]byte, StreamPageSize)
		offset := int64(pageNum * StreamPageSize)
		_, err := esf.ReadAt(readBuf, offset)

		if err != nil {
			t.Fatalf("ReadAt failed for page %d: %v", pageNum, err)
		}

		if !bytes.Equal(readBuf, expectedData) {
			t.Errorf("data mismatch for page %d", pageNum)
		}
	}
}

func TestEncryptedStreamFile_OffsetAdjustment(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header
	err = esf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write page at offset 0
	pageData := make([]byte, StreamPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	_, err = esf.WriteAt(pageData, 0)

	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Verify encrypted data is written at offset 64 (after header) in the underlying file
	underlyingBuf := make([]byte, StreamPageSize)
	n, err := file.ReadAt(underlyingBuf, StreamHeaderSize)

	if err != nil {
		t.Fatalf("failed to read from underlying file: %v", err)
	}

	if n != StreamPageSize {
		t.Errorf("expected %d bytes at offset %d, got %d", StreamPageSize, StreamHeaderSize, n)
	}

	// Verify the data is encrypted (should not match plaintext)
	if bytes.Equal(underlyingBuf, pageData) {
		t.Error("underlying file should contain encrypted data, not plaintext")
	}
}

func TestEncryptedStreamFile_InvalidPageSize(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header first
	if err := esf.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Variable-size writes should now work (padded to 4096 bytes internally)
	smallData := []byte("test data")
	n, err := esf.WriteAt(smallData, 0)

	if err != nil {
		t.Errorf("expected variable-size write to work, got error: %v", err)
	}

	if n != len(smallData) {
		t.Errorf("expected to write %d bytes, wrote %d", len(smallData), n)
	}

	// Variable-size reads should also work
	smallBuf := make([]byte, len(smallData))
	n, err = esf.ReadAt(smallBuf, 0)

	if err != nil {
		t.Errorf("expected variable-size read to work, got error: %v", err)
	}

	if n != len(smallData) {
		t.Errorf("expected to read %d bytes, read %d", len(smallData), n)
	}

	if string(smallBuf) != string(smallData) {
		t.Errorf("data mismatch: expected %q, got %q", smallData, smallBuf)
	}

	// Stream encryption now supports arbitrary-length writes
	// Large writes should work fine
	largeData := make([]byte, 5000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	_, err = esf.WriteAt(largeData, 0)

	if err != nil {
		t.Errorf("expected large write to work with stream encryption, got error: %v", err)
	}
}

func TestEncryptedStreamFile_UnalignedOffset(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header first
	if err := esf.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Unaligned writes should now work (using read-modify-write)
	data := []byte("test data at offset 512")
	n, err := esf.WriteAt(data, 512)

	if err != nil {
		t.Errorf("expected unaligned write to work, got error: %v", err)
	}

	if n != len(data) {
		t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
	}

	// Unaligned reads should also work
	readBuf := make([]byte, len(data))
	n, err = esf.ReadAt(readBuf, 512)

	if err != nil {
		t.Errorf("expected unaligned read to work, got error: %v", err)
	}

	if n != len(data) {
		t.Errorf("expected to read %d bytes, read %d", len(data), n)
	}

	if string(readBuf) != string(data) {
		t.Errorf("data mismatch: expected %q, got %q", data, readBuf)
	}
}

func TestEncryptedStreamFile_ReadAtEOF(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Write header only
	err = esf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Try to read page that doesn't exist
	readBuf := make([]byte, StreamPageSize)
	_, err = esf.ReadAt(readBuf, 0)

	if err != io.EOF {
		t.Errorf("expected io.EOF when reading non-existent page, got %v", err)
	}
}

func TestEncryptedStreamFile_UnsupportedOperations(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create underlying file
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close test file: %v", err)
		}
	}()

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Create encrypted stream file
	esf, err := NewEncryptedStreamFile(file, dataKey, keyHash, 123456, "/test.wal")

	if err != nil {
		t.Fatalf("NewEncryptedStreamFile failed: %v", err)
	}

	// Test unsupported operations
	buf := make([]byte, 100)

	// Read is now supported (delegates to ReadAt)
	// Seek to beginning first
	_, err = esf.Seek(0, io.SeekStart)

	if err != nil {
		t.Fatalf("failed to seek: %v", err)
	}

	// Read should work now
	_, err = esf.Read(buf)

	// Might get EOF or nil error depending on file state
	if err != nil && err != io.EOF {
		t.Errorf("expected Read to be supported, got error: %v", err)
	}

	// Write is now supported (delegates to WriteAt)
	buf = make([]byte, 4096)

	for i := range buf {
		buf[i] = byte(i % 256)
	}

	n, err := esf.Write(buf)

	if err != nil {
		t.Errorf("expected Write to be supported, got error: %v", err)
	}

	if n != 4096 {
		t.Errorf("expected Write to write 4096 bytes, got %d", n)
	}

	// Seek is now supported for PageLog compatibility
	offset, err := esf.Seek(0, io.SeekStart)

	if err != nil {
		t.Errorf("expected Seek to be supported, got error: %v", err)
	}

	if offset != 0 {
		t.Errorf("expected Seek to return 0, got %d", offset)
	}

	_, err = esf.WriteTo(nil)

	if err == nil {
		t.Error("expected error for WriteTo, got nil")
	}

	_, err = esf.WriteString("test")

	if err == nil {
		t.Error("expected error for WriteString, got nil")
	}
}
