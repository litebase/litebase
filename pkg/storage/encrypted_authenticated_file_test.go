package storage

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptedAuthenticatedFile_NewAndWriteHeader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Write header
	err = eaf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Verify header was written
	headerBuf := make([]byte, AuthenticatedHeaderSize)
	n, err := file.ReadAt(headerBuf, 0)

	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if n != AuthenticatedHeaderSize {
		t.Errorf("expected header size %d, got %d", AuthenticatedHeaderSize, n)
	}

	// Verify magic bytes
	magic := string(headerBuf[0:4])

	if magic != AuthenticatedHeaderMagic {
		t.Errorf("expected magic %q, got %q", AuthenticatedHeaderMagic, magic)
	}
}

func TestEncryptedAuthenticatedFile_OpenAndVerifyHeader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

		eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

		if err != nil {
			t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
		}

		err = eaf.WriteHeader()

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

		eaf, err := OpenEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

		if err != nil {
			t.Fatalf("OpenEncryptedAuthenticatedFile failed: %v", err)
		}

		if eaf == nil {
			t.Fatal("expected non-nil EncryptedAuthenticatedFile")
		}

		if eaf.PageCount() != 0 {
			t.Errorf("expected page count 0, got %d", eaf.PageCount())
		}
	}
}

func TestEncryptedAuthenticatedFile_OpenWithWrongKey(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

		eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

		if err != nil {
			t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
		}

		err = eaf.WriteHeader()

		if err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close test file: %v", err)
		}
	}

	// Try to open with wrong key
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
			wrongKey[i] = byte(i + 1)
		}

		wrongKeyHash := sha256.Sum256(wrongKey)

		_, err = OpenEncryptedAuthenticatedFile(file, wrongKey, wrongKeyHash, "/test.range")

		if err == nil {
			t.Error("expected error when opening with wrong key, got nil")
		}
	}
}

func TestEncryptedAuthenticatedFile_WriteAtReadAt_RoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Write header
	err = eaf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Create test data (4096 bytes)
	pageData := make([]byte, AuthenticatedPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write page at offset 0
	n, err := eaf.WriteAt(pageData, 0)

	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != AuthenticatedPageSize {
		t.Errorf("expected to write %d bytes, wrote %d", AuthenticatedPageSize, n)
	}

	// Verify page count updated
	if eaf.PageCount() != 1 {
		t.Errorf("expected page count 1, got %d", eaf.PageCount())
	}

	// Read page back
	readBuf := make([]byte, AuthenticatedPageSize)
	n, err = eaf.ReadAt(readBuf, 0)

	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if n != AuthenticatedPageSize {
		t.Errorf("expected to read %d bytes, read %d", AuthenticatedPageSize, n)
	}

	// Verify round-trip
	if !bytes.Equal(readBuf, pageData) {
		t.Error("read data does not match written data")
	}
}

func TestEncryptedAuthenticatedFile_MultiplePages(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Write header
	err = eaf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write multiple pages
	numPages := 10

	for pageNum := 0; pageNum < numPages; pageNum++ {
		pageData := make([]byte, AuthenticatedPageSize)

		for i := range pageData {
			pageData[i] = byte((i + pageNum) % 256)
		}

		offset := int64(pageNum * AuthenticatedPageSize)
		_, err := eaf.WriteAt(pageData, offset)

		if err != nil {
			t.Fatalf("WriteAt failed for page %d: %v", pageNum, err)
		}
	}

	// Verify page count
	if eaf.PageCount() != uint64(numPages) {
		t.Errorf("expected page count %d, got %d", numPages, eaf.PageCount())
	}

	// Read and verify all pages
	for pageNum := 0; pageNum < numPages; pageNum++ {
		expectedData := make([]byte, AuthenticatedPageSize)

		for i := range expectedData {
			expectedData[i] = byte((i + pageNum) % 256)
		}

		readBuf := make([]byte, AuthenticatedPageSize)
		offset := int64(pageNum * AuthenticatedPageSize)
		_, err := eaf.ReadAt(readBuf, offset)

		if err != nil {
			t.Fatalf("ReadAt failed for page %d: %v", pageNum, err)
		}

		if !bytes.Equal(readBuf, expectedData) {
			t.Errorf("data mismatch for page %d", pageNum)
		}
	}
}

func TestEncryptedAuthenticatedFile_VariableLength(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Write header
	err = eaf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write page with highly compressible data (all zeros)
	zeroPage := make([]byte, AuthenticatedPageSize)
	_, err = eaf.WriteAt(zeroPage, 0)

	if err != nil {
		t.Fatalf("WriteAt failed for zero page: %v", err)
	}

	// Write page with less compressible data
	randomPage := make([]byte, AuthenticatedPageSize)

	for i := range randomPage {
		randomPage[i] = byte(i % 256)
	}

	_, err = eaf.WriteAt(randomPage, AuthenticatedPageSize)

	if err != nil {
		t.Fatalf("WriteAt failed for random page: %v", err)
	}

	// Verify both pages can be read correctly
	readBuf := make([]byte, AuthenticatedPageSize)

	// Read zero page
	_, err = eaf.ReadAt(readBuf, 0)

	if err != nil {
		t.Fatalf("ReadAt failed for zero page: %v", err)
	}

	if !bytes.Equal(readBuf, zeroPage) {
		t.Error("zero page data mismatch")
	}

	// Read random page
	_, err = eaf.ReadAt(readBuf, AuthenticatedPageSize)

	if err != nil {
		t.Fatalf("ReadAt failed for random page: %v", err)
	}

	if !bytes.Equal(readBuf, randomPage) {
		t.Error("random page data mismatch")
	}
}

func TestEncryptedAuthenticatedFile_ReopenAndRead(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

	// Create encryption key and hash
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	keyHash := sha256.Sum256(dataKey)

	// Write pages
	{
		file, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

		if err != nil {
			t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
		}

		err = eaf.WriteHeader()

		if err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}

		// Write 3 pages
		for i := 0; i < 3; i++ {
			pageData := make([]byte, AuthenticatedPageSize)

			for j := range pageData {
				pageData[j] = byte((j + i*100) % 256)
			}

			_, err = eaf.WriteAt(pageData, int64(i*AuthenticatedPageSize))

			if err != nil {
				t.Fatalf("WriteAt failed: %v", err)
			}
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close test file: %v", err)
		}
	}

	// Reopen and verify pages
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

		eaf, err := OpenEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

		if err != nil {
			t.Fatalf("OpenEncryptedAuthenticatedFile failed: %v", err)
		}

		if eaf.PageCount() != 3 {
			t.Errorf("expected page count 3, got %d", eaf.PageCount())
		}

		// Read and verify each page
		for i := 0; i < 3; i++ {
			expectedData := make([]byte, AuthenticatedPageSize)

			for j := range expectedData {
				expectedData[j] = byte((j + i*100) % 256)
			}

			readBuf := make([]byte, AuthenticatedPageSize)
			_, err := eaf.ReadAt(readBuf, int64(i*AuthenticatedPageSize))

			if err != nil {
				t.Fatalf("ReadAt failed for page %d: %v", i, err)
			}

			if !bytes.Equal(readBuf, expectedData) {
				t.Errorf("data mismatch for page %d", i)
			}
		}
	}
}

func TestEncryptedAuthenticatedFile_ReadNonExistentPage(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Write header
	err = eaf.WriteHeader()

	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Try to read page that doesn't exist
	readBuf := make([]byte, AuthenticatedPageSize)
	_, err = eaf.ReadAt(readBuf, 0)

	if err != io.EOF {
		t.Errorf("expected io.EOF when reading non-existent page, got %v", err)
	}
}

func TestEncryptedAuthenticatedFile_InvalidPageSize(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Try to write wrong size
	invalidData := make([]byte, 100)
	_, err = eaf.WriteAt(invalidData, 0)

	if err == nil {
		t.Error("expected error for invalid write size, got nil")
	}

	// Try to read wrong size
	invalidBuf := make([]byte, 100)
	_, err = eaf.ReadAt(invalidBuf, 0)

	if err == nil {
		t.Error("expected error for invalid read size, got nil")
	}
}

func TestEncryptedAuthenticatedFile_UnalignedOffset(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Try to write at unaligned offset
	pageData := make([]byte, AuthenticatedPageSize)
	_, err = eaf.WriteAt(pageData, 100)

	if err == nil {
		t.Error("expected error for unaligned offset, got nil")
	}

	// Try to read at unaligned offset
	_, err = eaf.ReadAt(pageData, 100)

	if err == nil {
		t.Error("expected error for unaligned offset, got nil")
	}
}

func TestEncryptedAuthenticatedFile_UnsupportedOperations(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.range")

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

	// Create encrypted authenticated file
	eaf, err := NewEncryptedAuthenticatedFile(file, dataKey, keyHash, "/test.range")

	if err != nil {
		t.Fatalf("NewEncryptedAuthenticatedFile failed: %v", err)
	}

	// Test unsupported operations
	buf := make([]byte, 100)

	_, err = eaf.Read(buf)

	if err == nil {
		t.Error("expected error for Read, got nil")
	}

	_, err = eaf.Write(buf)

	if err == nil {
		t.Error("expected error for Write, got nil")
	}

	_, err = eaf.Seek(0, io.SeekStart)

	if err == nil {
		t.Error("expected error for Seek, got nil")
	}

	err = eaf.Truncate(100)

	if err == nil {
		t.Error("expected error for Truncate, got nil")
	}

	_, err = eaf.WriteTo(nil)

	if err == nil {
		t.Error("expected error for WriteTo, got nil")
	}

	_, err = eaf.WriteString("test")

	if err == nil {
		t.Error("expected error for WriteString, got nil")
	}
}
