package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
)

type Encrypter struct {
	key    []byte
	cipher string
}

func NewEncrypter(key []byte) *Encrypter {
	hash := sha256.New()
	hash.Write(key)
	key = hash.Sum(nil)

	return &Encrypter{
		key:    key,
		cipher: "aes-256-gcm",
	}
}

func (encrypter *Encrypter) Decrypt(text string) (DecryptedSecret, error) {
	encrypted, err := base64.StdEncoding.DecodeString(text)

	if err != nil {
		return DecryptedSecret{}, err
	}

	block, err := aes.NewCipher(encrypter.key)

	if err != nil {
		log.Fatal(err)
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		log.Fatal(err)
	}

	iv := encrypted[:aead.NonceSize()]
	ciphertext := encrypted[aead.NonceSize() : len(encrypted)-aead.Overhead()]
	tag := encrypted[len(encrypted)-aead.Overhead():]
	ciphertext = append(ciphertext, tag...)
	plaintext, err := aead.Open(nil, iv, ciphertext, nil)

	if err != nil {
		return DecryptedSecret{}, err
	}

	return DecryptedSecret{
		Key:   base64.StdEncoding.EncodeToString(encrypter.key),
		Value: string(plaintext),
	}, nil
}

func (encrypter *Encrypter) Encrypt(plaintext string) (string, error) {
	plaintextBytes := []byte(plaintext)

	block, err := aes.NewCipher(encrypter.key)

	if err != nil {
		log.Fatal(err)
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		log.Fatal(err)
	}

	iv := make([]byte, aead.NonceSize())

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		log.Fatal(err)
	}

	ciphertext := aead.Seal(nil, iv, plaintextBytes, nil)
	value := ciphertext[:len(ciphertext)-aead.Overhead()]
	tag := ciphertext[len(ciphertext)-aead.Overhead():]
	encrypted := append(iv, append(value, tag...)...)

	return base64.StdEncoding.EncodeToString(encrypted), nil
}
