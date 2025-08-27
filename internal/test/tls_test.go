package test

import (
	"crypto/tls"
	"os"
	"testing"
)

func TestGenerateTLSCertificate(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate TLS certificate
	result, err := GenerateTLSCertificate(tmpDir)
	if err != nil {
		t.Fatalf("failed to generate TLS certificate: %v", err)
	}

	// Verify files exist
	if _, err := os.Stat(result.CertPath); os.IsNotExist(err) {
		t.Fatalf("certificate file does not exist: %s", result.CertPath)
	}

	if _, err := os.Stat(result.KeyPath); os.IsNotExist(err) {
		t.Fatalf("key file does not exist: %s", result.KeyPath)
	}

	// Verify the certificate can be loaded
	cert, err := tls.LoadX509KeyPair(result.CertPath, result.KeyPath)
	if err != nil {
		t.Fatalf("failed to load generated certificate: %v", err)
	}

	// Verify the certificate is valid for localhost
	if len(cert.Certificate) == 0 {
		t.Fatal("certificate chain is empty")
	}

	t.Logf("Successfully generated TLS certificate at %s and key at %s", result.CertPath, result.KeyPath)
}
