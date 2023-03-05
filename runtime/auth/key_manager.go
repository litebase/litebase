package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

var lockFile *os.File

func EncryptKey(signature, key string) (string, error) {
	plaintextBytes := []byte(key)
	hash := sha256.New()
	hash.Write([]byte(signature))
	secret := hash.Sum(nil)

	block, err := aes.NewCipher(secret)

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

	ciphertext := aead.Seal(nil, iv, plaintextBytes, nil)
	value := ciphertext[:len(ciphertext)-aead.Overhead()]
	tag := ciphertext[len(ciphertext)-aead.Overhead():]
	encrypted := append(iv, append(value, tag...)...)

	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func generatePrivateKey(signature string) (*rsa.PrivateKey, error) {
	log.Println("Generating private key for", signature)
	var err error

	if _, err := os.Stat(KeyPath("private", signature)); err == nil {
		return nil, errors.New("private key already exists")
	}

	// Create the signature directory if it does not exist
	signatureDirectory := Path(signature)

	if _, err := os.Stat(signatureDirectory); os.IsNotExist(err) {
		if err := os.MkdirAll(signatureDirectory, 0755); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	// Get the lock file
	lockFile, err = os.OpenFile(lockPath(signature), os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer lockFile.Close()

	// Attempt to get an exclusive lock on the file
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Unlock the file when we're done
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)

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
	privateKey, err := GetPrivateKey(signature)

	if err != nil {
		return nil, err
	}

	return &privateKey.PublicKey, nil
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

func NextSignature(signature string) (string, error) {
	if config.Get("signature") == signature {
		publickey, err := GetRawPublicKey(signature)

		if err == nil {
			return EncryptKey(signature, string(publickey))
		}
	}

	config.Set("signature_next", signature)

	_, err := generatePrivateKey(signature)

	if err != nil {
		return "", err
	}

	err = rotate()

	if err != nil {
		log.Println(err)
		return "", err
	}

	publicKey, err := GetRawPublicKey(signature)

	if err != nil {
		return "", err
	}

	return EncryptKey(signature, string(publicKey))
}

func Path(signature string) string {
	return strings.Join([]string{
		config.Get("data_path"),
		".litebasedb",
		signature,
	}, "/")
}

func rotate() error {
	if config.Get("signature_next") == "" {
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

	if err := os.WriteFile(Path(fmt.Sprintf("%s/%s", config.Get("signature_next"), ".rotate-lock")), []byte{}, 0666); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var errors = []error{}

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := rotateAccessKeys()

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := rotateSettings()

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Wait()

	for _, err := range errors {
		return err
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

	for _, settingsDirectory := range settings {
		// Get the file in the settings directory
		settingsDirectoryFiles, err := os.ReadDir(strings.Join([]string{
			settingsDir,
			settingsDirectory.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		var settingsFileName string

		for _, settingsFile := range settingsDirectoryFiles {
			settingsFileName = settingsFile.Name()
			break
		}

		if settingsFileName == "" {
			return errors.New("no settings file found")
		}

		settingBytes, err := os.ReadFile(strings.Join([]string{
			settingsDir,
			settingsDirectory.Name(),
			settingsFileName,
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

		if err := os.MkdirAll(strings.Join([]string{
			newSettingsDir,
			settingsDirectory.Name(),
		}, "/"), 0755); err != nil {
			return err
		}

		if err := os.WriteFile(strings.Join([]string{
			newSettingsDir,
			settingsDirectory.Name(),
			settingsFileName,
		}, "/"), []byte(encryptedSetting), 0666); err != nil {
			return err
		}
	}

	return nil
}
