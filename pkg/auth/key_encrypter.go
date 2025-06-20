package auth

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
)

type KeyEncrypter struct {
	secretsManager *SecretsManager
	signature      string
}

func NewKeyEncrypter(secretsManager *SecretsManager, signature string) *KeyEncrypter {
	return &KeyEncrypter{
		secretsManager: secretsManager,
		signature:      signature,
	}
}

func (k *KeyEncrypter) Decrypt(data []byte) (DecryptedSecret, error) {
	payload := make([]byte, base64.StdEncoding.DecodedLen(len(data)))

	_, err := base64.StdEncoding.Decode(payload, data)

	if err != nil {
		return DecryptedSecret{}, err
	}

	var decoded map[string]string

	err = json.Unmarshal(bytes.Trim(payload, "\x00"), &decoded)

	if err != nil {
		return DecryptedSecret{}, err
	}

	privateKey, err := k.privateKey()

	if err != nil {
		return DecryptedSecret{}, err
	}

	enryptedKey, err := base64.StdEncoding.DecodeString(decoded["key"])

	if err != nil {
		return DecryptedSecret{}, err
	}

	decryptedKey, err := rsa.DecryptPKCS1v15(
		rand.Reader,
		privateKey,
		enryptedKey,
	)

	if err != nil {
		return DecryptedSecret{}, err
	}

	block, err := aes.NewCipher(decryptedKey)

	if err != nil {
		return DecryptedSecret{}, err
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return DecryptedSecret{}, err
	}

	encrypted, err := base64.StdEncoding.DecodeString(decoded["value"])

	if err != nil {
		return DecryptedSecret{}, err
	}

	iv := encrypted[:aead.NonceSize()]
	ciphertext := encrypted[aead.NonceSize() : len(encrypted)-aead.Overhead()]
	tag := encrypted[len(encrypted)-aead.Overhead():]
	ciphertext = append(ciphertext, tag...)
	decrypted, err := aead.Open(nil, iv, ciphertext, nil)

	if err != nil {
		return DecryptedSecret{}, err
	}

	return DecryptedSecret{
		Key:   base64.StdEncoding.EncodeToString(decryptedKey),
		Value: string(decrypted),
	}, nil
}

func (k *KeyEncrypter) Encrypt(data []byte) ([]byte, error) {
	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)

	if err != nil {
		return nil, err
	}

	hash := sha256.New()
	hash.Write(secretKey)
	key := hash.Sum(nil)

	// Encrypt the key with the public key
	privateKey, err := k.privateKey()

	if err != nil {
		return nil, err
	}

	encryptedSecretKey, err := rsa.EncryptPKCS1v15(
		rand.Reader,
		&privateKey.PublicKey,
		key,
	)

	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)

	if err != nil {
		return nil, err
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, err
	}

	iv := make([]byte, aead.NonceSize())

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	ciphertext := aead.Seal(nil, iv, []byte(data), nil)
	value := ciphertext[:len(ciphertext)-aead.Overhead()]
	tag := ciphertext[len(ciphertext)-aead.Overhead():]
	encrypted := append(iv, append(value, tag...)...)

	jsonEncoded, err := json.Marshal(map[string]string{
		"key":   base64.StdEncoding.EncodeToString(encryptedSecretKey),
		"value": base64.StdEncoding.EncodeToString(encrypted),
	})

	if err != nil {
		return nil, err
	}

	dst := make([]byte, base64.StdEncoding.EncodedLen(len(jsonEncoded)))

	base64.StdEncoding.Encode(dst, jsonEncoded)

	return dst, nil
}

func (k *KeyEncrypter) privateKey() (*rsa.PrivateKey, error) {
	return GetPrivateKey(k.signature, k.secretsManager.ObjectFS)
}
