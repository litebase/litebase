package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"litebasedb/router/config"
	"log"
	"os"
	"strings"
	"sync"
	"time"
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

	err := Rotate()

	if err != nil {
		log.Fatal(err)
	}

	return publicKeyString
}

func Path(signature string) string {
	return strings.Join([]string{
		config.Get("data_path"),
		".litebasedb",
		signature,
	}, "/")
}

func Rotate() error {
	if config.Get("signature_next") == "" {
		return nil
	}

	if _, err := os.Stat(KeyPath("private", config.Get("signature_next"))); err == nil {
		return nil
	}

	if _, err := os.Stat(Path(config.Get("signature_next") + "/.rotate-lock")); err == nil {
		return nil
	}

	if _, err := os.Stat(Path(config.Get("signature_next") + "/manifest.json")); err == nil {
		return nil
	}

	// create rotate lock
	if err := os.MkdirAll(Path(config.Get("signature_next")), 0755); err != nil {
		return err
	}

	if err := os.WriteFile(Path(config.Get("signature_next")+"/.rotate-lock"), []byte{}, 0666); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var errorChan = make(chan error)

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := rotateAccessKeys()

		if err != nil {
			errorChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		err := rotateSettings()

		if err != nil {
			errorChan <- err
		}
	}()

	wg.Wait()

	close(errorChan)

	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	manifest := map[string]interface{}{
		"signature":  config.Get("signature"),
		"rotated_at": time.Now().Unix(),
	}

	manifestBytes, err := json.Marshal(manifest)

	if err != nil {
		return err
	}

	if err := os.WriteFile(Path(config.Get("signature_next")+"/manifest.json"), manifestBytes, 0666); err != nil {
		return err
	}

	if err := os.Remove(Path(config.Get("signature_next") + "/.rotate-lock")); err != nil {
		return err
	}

	return nil
}

func rotateAccessKeys() error {
	accessKeyDir := strings.Join([]string{
		Path(config.Get("signature")),
		"access_keys",
	}, "/")

	newAccessKeyDir := strings.Join([]string{
		Path(config.Get("signature_next")),
		"access_keys",
	}, "/")

	accessKeys, err := os.ReadDir(accessKeyDir)

	if err != nil {
		return err
	}

	if err := os.MkdirAll(newAccessKeyDir, 0755); err != nil {
		return err
	}

	for _, accessKey := range accessKeys {
		accessKeyBytes, err := os.ReadFile(strings.Join([]string{
			accessKeyDir,
			accessKey.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		decryptedAccessKey, err := SecretsManager().Decrypt(config.Get("signature"), string(accessKeyBytes))

		if err != nil {
			return err
		}

		encryptedAccessKey, err := SecretsManager().Encrypt(config.Get("signature_next"), decryptedAccessKey["value"])

		if err != nil {
			return err
		}

		if err := os.WriteFile(strings.Join([]string{
			newAccessKeyDir,
			accessKey.Name(),
		}, "/"), []byte(encryptedAccessKey), 0666); err != nil {
			return err
		}
	}

	return nil
}

func KeyManagerInit() {

	if _, err := os.Stat(Path(config.Get("signature"))); err == os.ErrNotExist {
		_, err := GeneratePrivateKey(config.Get("signature"))

		if err != nil {
			return
		}

		fmt.Println("Public Key:")
		file, err := os.ReadFile(KeyPath("public", config.Get("signature")))

		if err != nil {
			log.Fatal(err)
			return
		}

		fmt.Println(EncryptKey(string(file)))
	}

	Rotate()
}

func rotateSettings() error {
	settingsDir := strings.Join([]string{
		Path(config.Get("signature")),
		"settings",
	}, "/")

	newSettingsDir := strings.Join([]string{
		Path(config.Get("signature_next")),
		"settings",
	}, "/")

	settings, err := os.ReadDir(settingsDir)

	if err != nil {
		return err
	}

	if err := os.MkdirAll(newSettingsDir, 0755); err != nil {
		return err
	}

	for _, setting := range settings {
		settingBytes, err := os.ReadFile(strings.Join([]string{
			settingsDir,
			setting.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		decryptedSetting, err := SecretsManager().Decrypt(config.Get("signature"), string(settingBytes))

		if err != nil {
			return err
		}

		encryptedSetting, err := SecretsManager().Encrypt(config.Get("signature_next"), decryptedSetting["value"])

		if err != nil {
			return err
		}

		if err := os.WriteFile(strings.Join([]string{
			newSettingsDir,
			setting.Name(),
		}, "/"), []byte(encryptedSetting), 0666); err != nil {
			return err
		}
	}

	return nil
}
