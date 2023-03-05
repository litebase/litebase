package auth

import (
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
	databaseUuid string
	publicKey    *rsa.PublicKey
	signature    string
}

func NewKeyEncrypter(signature string) *KeyEncrypter {
	return &KeyEncrypter{
		signature: signature,
	}
}

func (k *KeyEncrypter) Decrypt(data string) (map[string]string, error) {
	payload, err := base64.StdEncoding.DecodeString(data)

	if err != nil {
		return nil, err
	}

	var decoded map[string]string

	err = json.Unmarshal(payload, &decoded)

	if err != nil {
		return nil, err
	}

	privateKey, err := k.privateKey()

	if err != nil {
		return nil, err
	}

	encryptedSecretKey, err := base64.StdEncoding.DecodeString(decoded["key"])

	if err != nil {
		return nil, err
	}

	decryptedKey, err := rsa.DecryptPKCS1v15(
		rand.Reader,
		privateKey,
		encryptedSecretKey,
	)

	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(decryptedKey)

	if err != nil {
		return nil, err
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, err
	}

	encrypted, err := base64.StdEncoding.DecodeString(decoded["value"])

	if err != nil {
		return nil, err
	}

	iv := encrypted[:aead.NonceSize()]
	ciphertext := encrypted[aead.NonceSize() : len(encrypted)-aead.Overhead()]
	tag := encrypted[len(encrypted)-aead.Overhead():]
	ciphertext = append(ciphertext, tag...)
	decrypted, err := aead.Open(nil, iv, ciphertext, nil)

	if err != nil {
		return nil, err
	}

	return map[string]string{
		"key":   base64.StdEncoding.EncodeToString(decryptedKey),
		"value": string(decrypted),
	}, nil
}

func (k *KeyEncrypter) Encrypt(data string) (string, error) {
	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)

	if err != nil {
		return "", err
	}

	hash := sha256.New()
	hash.Write(secretKey)
	key := hash.Sum(nil)

	// Encrypt the key with the public key
	publicKey, err := k.PublicKey()

	if err != nil {
		return "", err
	}

	encryptedSecretKey, err := rsa.EncryptPKCS1v15(
		rand.Reader,
		publicKey,
		key,
	)

	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)

	if err != nil {
		return "", err
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return "", err
	}

	iv := make([]byte, aead.NonceSize())

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
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
		return "", err
	}

	return base64.StdEncoding.EncodeToString(jsonEncoded), nil
}

func (k *KeyEncrypter) ForDatabase(databaseUuid string) *KeyEncrypter {
	k.databaseUuid = databaseUuid

	return k
}

func (k *KeyEncrypter) privateKey() (*rsa.PrivateKey, error) {
	return GetPrivateKey(k.signature)
}

func (k *KeyEncrypter) PublicKey() (*rsa.PublicKey, error) {
	var err error

	if k.publicKey == nil {
		if k.databaseUuid != "" {
			k.publicKey, err = GetPublicKeyForDatabase(k.signature, k.databaseUuid)
		} else {
			k.publicKey, err = GetPublicKey(k.signature)
		}
	}

	return k.publicKey, err
}
