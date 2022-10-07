package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
)

type Encrypter struct {
	key    []byte
	cipher string
}

func NewEncrypter(key []byte) *Encrypter {
	return &Encrypter{
		key:    key,
		cipher: "aes-256-gcm",
	}
}

func (encrypter *Encrypter) Encrypt(text string) (string, error) {
	nonce := make([]byte, 12)

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	plaintext := []byte(text)

	block, err := aes.NewCipher(encrypter.key)

	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)

	if err != nil {
		panic(err.Error())
	}

	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)

	json, err := json.Marshal(map[string]string{
		"nonce": base64.StdEncoding.EncodeToString(nonce),
		"value": base64.StdEncoding.EncodeToString(ciphertext),
	})

	return base64.StdEncoding.EncodeToString(json), nil
}

func (encrypter *Encrypter) Decrypt(text string) (string, error) {
	decodedText, err := base64.StdEncoding.DecodeString(text)

	if err != nil {
		panic(err.Error())
	}

	var data map[string]string

	json.Unmarshal(decodedText, &data)

	nonce, err := base64.StdEncoding.DecodeString(data["nonce"])

	if err != nil {
		panic(err.Error())
	}

	ciphertext, err := base64.StdEncoding.DecodeString(data["value"])

	if err != nil {
		panic(err.Error())
	}

	block, err := aes.NewCipher(encrypter.key)

	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)

	if err != nil {
		panic(err.Error())
	}

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)

	if err != nil {
		panic(err.Error())
	}

	return string(plaintext), nil
}
