package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"litebasedb/runtime/config"
	"log"
	"os"
	"strings"
)

func EncryptKey(key string) (string, error) {
	encrypter := NewEncrypter([]byte(config.Get("signature")))

	return encrypter.Encrypt(key)
}

func GeneratePrivateKey(signature string) (*rsa.PrivateKey, error) {
	if _, err := os.Stat(KeyPath("private", signature)); err == nil {
		return nil, errors.New("private key already exists")
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(Path(signature), 0755); err != nil {
		return nil, err
	}

	file, err := os.Create(KeyPath("private", signature))

	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Write the key to the file
	if err := pem.Encode(file, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}); err != nil {
		return nil, err
	}

	file, err = os.Create(KeyPath("public", signature))

	if err != nil {
		return nil, err
	}

	defer file.Close()

	if err != nil {
		return nil, err
	}

	publicKey, err := x509.MarshalPKIXPublicKey(&key.PublicKey)

	if err != nil {
		return nil, err
	}

	if err := pem.Encode(file, &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKey,
	}); err != nil {
		return nil, err
	}

	return key, nil
}

func GetPrivateKey(signature string) (*rsa.PrivateKey, error) {
	privateKey, err := os.ReadFile(KeyPath("private", signature))

	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(privateKey)

	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)

	if err != nil {
		return nil, err
	}

	return key, nil
}

func GetPublicKey(signature string) (*rsa.PublicKey, error) {
	path := KeyPath("public", signature)

	publicKey, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(publicKey)

	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)

	if err != nil {
		return nil, err
	}

	return key.(*rsa.PublicKey), nil
}

func GetRawPublicKey(signature string) ([]byte, error) {
	path := KeyPath("public", signature)

	file, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func Init() {}

func KeyPath(keyType string, signature string) string {
	return strings.Join([]string{
		Path(signature),
		fmt.Sprintf("%s.key", keyType),
	}, "/")
}

func lockPath(signature string) string {
	return strings.Join([]string{
		Path(signature),
		"private.lock",
	}, "/")
}

func NextSignature(signature string) string {
	publicKey := config.Swap("signature_next", signature, func() interface{} {
		privateKey, err := GeneratePrivateKey(signature)

		if err != nil {
			log.Fatal(err)
		}

		return privateKey.PublicKey
	})

	publicKeyString := publicKey.(*rsa.PublicKey).N.String()

	return publicKeyString
}

func Path(signature string) string {
	return strings.Join([]string{
		config.Get("data_path"),
		".litebasedb",
		signature,
	}, "/")
}

func Rotate() {
}

func rotateAccessKeys() {
}

func rotateSettings() {
}
