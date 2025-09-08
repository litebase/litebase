package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// TLSCertificateResult contains the paths to generated certificate files
type TLSCertificateResult struct {
	CertPath string
	KeyPath  string
}

// GenerateTLSCertificate generates a self-signed TLS certificate for localhost testing
// Returns the paths to the certificate and key files
func GenerateTLSCertificate(tmpDir string) (*TLSCertificateResult, error) {
	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)

	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Litebase"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Atlanta, GA"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:    []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	// Write certificate to file
	certPath := filepath.Join(tmpDir, "cert.pem")
	certFile, err := os.Create(certPath)

	if err != nil {
		return nil, fmt.Errorf("failed to create certificate file: %w", err)
	}

	defer func() {
		if err := certFile.Close(); err != nil {
			slog.Error("failed to close certificate file:", "error", err)
		}
	}()

	if err := pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	}); err != nil {
		return nil, fmt.Errorf("failed to write certificate: %w", err)
	}

	// Write private key to file
	keyPath := filepath.Join(tmpDir, "key.pem")
	keyFile, err := os.Create(keyPath)

	if err != nil {
		return nil, fmt.Errorf("failed to create key file: %w", err)
	}

	defer func() {
		if err := keyFile.Close(); err != nil {
			slog.Error("failed to close key file:", "error", err)
		}
	}()

	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	if err := pem.Encode(keyFile, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privateKeyDER,
	}); err != nil {
		return nil, fmt.Errorf("failed to write private key: %w", err)
	}

	return &TLSCertificateResult{
		CertPath: certPath,
		KeyPath:  keyPath,
	}, nil
}
