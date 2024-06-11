package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"litebase/internal/config"
	"litebase/server/storage"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var privateKeys = map[string]*rsa.PrivateKey{}
var privateKeysMutex = &sync.Mutex{}

func EncryptKey(signature, key string) (string, error) {
	plaintextBytes := []byte(key)
	secret := generateHash(signature)

	block, err := aes.NewCipher([]byte(secret))

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

// Generate a new key for the current signature if one does not exist.
func generate() {
	_, err := storage.FS().Stat(KeyPath("public", config.Get().Signature))

	if os.IsNotExist(err) {
		_, err := generatePrivateKey(config.Get().Signature)

		if err != nil {
			log.Fatal(err)
			return
		}

		file, err := storage.FS().ReadFile(KeyPath("public", config.Get().Signature))

		if err != nil {
			log.Fatal(err)
			return
		}

		_, err = EncryptKey(config.Get().Signature, string(file))

		if err != nil {
			log.Fatal(err)
			return
		}

		// fmt.Println("\n------------")

		// fmt.Println("Public Key:")

		// fmt.Println("\n" + key + "\n")

		// fmt.Println("\n------------")

	}
}

// Generate a hash of the signature so that it is not stored in plain text.
func generateHash(signature string) string {
	hash := sha256.Sum256([]byte(signature))

	return hex.EncodeToString(hash[:])
}

func generatePrivateKey(signature string) (*rsa.PrivateKey, error) {
	var err error

	if _, err := storage.FS().Stat(KeyPath("private", signature)); err == nil {
		return nil, errors.New("private key already exists")
	}

	// Create the signature directory if it does not exist
	signatureDirectory := Path(signature)

	if _, err := storage.FS().Stat(signatureDirectory); os.IsNotExist(err) {
		if err := storage.FS().MkdirAll(signatureDirectory, 0755); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	// // Get the lock file
	// lockFile, err = os.OpenFile(lockPath(signature), os.O_CREATE|os.O_RDWR, 0644)

	// if err != nil {
	// 	log.Println(err)
	// 	return nil, err
	// }

	// defer lockFile.Close()

	// // Attempt to get an exclusive lock on the file
	// err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	// if err != nil {
	// 	log.Println(err)
	// 	return nil, err
	// }

	// // Unlock the file when we're done
	// defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)

	key, err := rsa.GenerateKey(rand.Reader, 2048)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := storage.FS().MkdirAll(Path(signature), 0755); err != nil {
		log.Println(err)
		return nil, err
	}

	file, err := storage.FS().Create(KeyPath("private", signature))

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer file.Close()

	// Write the key to the file
	if err := pem.Encode(file, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}); err != nil {
		log.Println("ERROR", err)
		return nil, err
	}

	file, err = storage.FS().Create(KeyPath("public", signature))

	if err != nil {
		return nil, err
	}

	defer file.Close()

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
	privateKeysMutex.Lock()
	defer privateKeysMutex.Unlock()

	if privateKeys[signature] == nil {
		privateKey, err := storage.FS().ReadFile(KeyPath("private", signature))

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

		privateKeys[signature] = key
	}

	return privateKeys[signature], nil
}

func GetPublicKey(signature string) (*rsa.PublicKey, error) {
	path := KeyPath("public", signature)

	publicKey, err := storage.FS().ReadFile(path)

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

func GetPublicKeyForDatabase(signature, databaseUuid string) (*rsa.PublicKey, error) {
	publicKey, err := SecretsManager().GetPublicKey(signature, databaseUuid)

	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode([]byte(publicKey))

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

	file, err := storage.FS().ReadFile(path)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func KeyManagerInit() {
	// Generate a public key for the signature if one does not exist
	generate()
	// Rotate the keys if a new signature is set
	rotate()
}

func KeyPath(keyType string, signature string) string {
	return strings.Join([]string{
		Path(signature),
		fmt.Sprintf("%s.key", keyType),
	}, "/")
}

func NextSignature(signature string) (string, error) {
	if config.Get().Signature == signature {
		publickey, err := GetRawPublicKey(signature)

		if err == nil {
			return EncryptKey(signature, string(publickey))
		}
	}

	config.Get().SignatureNext = signature

	_, err := generatePrivateKey(signature)

	if err != nil {
		return "", err
	}

	err = rotate()

	if err != nil {
		log.Println(err)
		return "", err
	}

	Broadcast("next_signature", signature)

	publicKey, err := GetRawPublicKey(signature)

	if err != nil {
		return "", err
	}

	return EncryptKey(signature, string(publicKey))
}

func Path(signature string) string {
	return strings.Join([]string{
		config.Get().DataPath,
		".litebase",
		generateHash(signature),
	}, "/")
}

func rotate() error {
	if config.Get().SignatureNext == "" {
		return nil
	}

	if _, err := storage.FS().Stat(Path(fmt.Sprintf("%s/%s", config.Get().SignatureNext, ".rotate-lock"))); err == nil {
		return nil
	}

	if _, err := storage.FS().Stat(Path(fmt.Sprintf("%s/%s", config.Get().SignatureNext, "manifest.json"))); err == nil {
		return nil
	}

	// create rotate lock
	if err := os.MkdirAll(Path(config.Get().SignatureNext), 0755); err != nil {
		return err
	}

	if err := storage.FS().WriteFile(Path(fmt.Sprintf("%s/%s", config.Get().SignatureNext, ".rotate-lock")), []byte{}, 0666); err != nil {
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

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := rotateDatabaseKeys()

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Wait()

	for _, err := range errors {
		return err
	}

	manifest := map[string]interface{}{
		"signature":  config.Get().Signature,
		"rotated_at": time.Now().Unix(),
	}

	manifestBytes, err := json.Marshal(manifest)

	if err != nil {
		return err
	}

	if err := storage.FS().WriteFile(Path(config.Get().SignatureNext+"/manifest.json"), manifestBytes, 0666); err != nil {
		return err
	}

	if err := os.Remove(Path(fmt.Sprintf("%s/%s", config.Get().SignatureNext, ".rotate-lock"))); err != nil {
		return err
	}

	return nil
}

func rotateAccessKeys() error {
	accessKeyDir := strings.Join([]string{
		Path(config.Get().Signature),
		"access_keys",
	}, "/")

	newAccessKeyDir := strings.Join([]string{
		Path(config.Get().SignatureNext),
		"access_keys",
	}, "/")

	accessKeys, err := storage.FS().ReadDir(accessKeyDir)

	if err != nil {
		return err
	}

	if err := storage.FS().MkdirAll(newAccessKeyDir, 0755); err != nil {
		return err
	}

	for _, accessKey := range accessKeys {
		accessKeyBytes, err := storage.FS().ReadFile(strings.Join([]string{
			accessKeyDir,
			accessKey.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		decryptedAccessKey, err := SecretsManager().Decrypt(config.Get().Signature, string(accessKeyBytes))

		if err != nil {
			return err
		}

		encryptedAccessKey, err := SecretsManager().Encrypt(config.Get().SignatureNext, decryptedAccessKey["value"])

		if err != nil {
			return err
		}

		if err := storage.FS().WriteFile(strings.Join([]string{
			newAccessKeyDir,
			accessKey.Name(),
		}, "/"), []byte(encryptedAccessKey), 0666); err != nil {
			return err
		}
	}

	return nil
}

func rotateDatabaseKeys() error {
	keysDir := strings.Join([]string{
		Path(config.Get().Signature),
		"database_keys",
	}, "/")

	newKeysDir := strings.Join([]string{
		Path(config.Get().SignatureNext),
		"database_keys",
	}, "/")

	keys, err := storage.FS().ReadDir(keysDir)

	if err != nil {
		return err
	}

	if err := storage.FS().MkdirAll(newKeysDir, 0755); err != nil {
		return err
	}

	for _, key := range keys {
		databaseKeyBytes, err := storage.FS().ReadFile(strings.Join([]string{
			keysDir,
			key.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		if err := storage.FS().WriteFile(strings.Join([]string{
			newKeysDir,
			key.Name(),
		}, "/"), databaseKeyBytes, 0666); err != nil {
			return err
		}
	}

	return nil
}

func rotateSettings() error {
	var databaseSettings []os.DirEntry

	settingsDir := strings.Join([]string{
		Path(config.Get().Signature),
		"settings",
	}, "/")

	newSettingsDir := strings.Join([]string{
		Path(config.Get().SignatureNext),
		"settings",
	}, "/")

	settings, err := storage.FS().ReadDir(settingsDir)

	if err != nil {
		return err
	}

	if err := storage.FS().MkdirAll(newSettingsDir, 0755); err != nil {
		return err
	}

	for _, setting := range settings {
		if err := storage.FS().MkdirAll(strings.Join([]string{
			newSettingsDir,
			setting.Name(),
		}, "/"), 0755); err != nil {
			return err
		}

		databaseSettings, err = storage.FS().ReadDir(strings.Join([]string{
			settingsDir,
			setting.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		for _, databaseSetting := range databaseSettings {
			databaseSettingBytes, err := storage.FS().ReadFile(strings.Join([]string{
				settingsDir,
				setting.Name(),
				databaseSetting.Name(),
			}, "/"))

			if err != nil {
				return err
			}

			decryptedSetting, err := SecretsManager().Decrypt(config.Get().Signature, string(databaseSettingBytes))

			if err != nil {
				return err
			}

			encryptedSetting, err := SecretsManager().Encrypt(config.Get().SignatureNext, decryptedSetting["value"])

			if err != nil {
				return err
			}

			if err := storage.FS().WriteFile(strings.Join([]string{
				newSettingsDir,
				setting.Name(),
				databaseSetting.Name(),
			}, "/"), []byte(encryptedSetting), 0666); err != nil {
				return err
			}
		}
	}

	return nil
}
