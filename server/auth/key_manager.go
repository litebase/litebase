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
	"litebase/internal/config"
	internalStorage "litebase/internal/storage"
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

	secret := sha256.Sum256([]byte(signature))

	block, err := aes.NewCipher(secret[:])

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
func generate(c *config.Config, objectFS *storage.FileSystem) error {
	signature := c.Signature

	// Ensure the signature is a 32 byte hash
	if len(signature) != 64 {
		return errors.New("invalid signature length")
	}

	_, err := objectFS.Stat(KeyPath("public", signature))

	if os.IsNotExist(err) {
		_, err := generatePrivateKey(signature, objectFS)

		if err != nil {
			return err
		}

		file, err := objectFS.ReadFile(KeyPath("public", signature))

		if err != nil {
			return err
		}

		_, err = EncryptKey(signature, string(file))

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func generatePrivateKey(signature string, objectFS *storage.FileSystem) (*rsa.PrivateKey, error) {
	var err error

	if _, err := objectFS.Stat(KeyPath("private", signature)); err == nil {
		return nil, errors.New("private key already exists")
	}

	// Create the signature directory if it does not exist
	signatureDirectory := Path(signature)

	if _, err := objectFS.Stat(signatureDirectory); os.IsNotExist(err) {
		if err := objectFS.MkdirAll(signatureDirectory, 0755); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	// // Get the lock file
	// lockFile, err = objectFS.OpenFile(lockPath(signature), os.O_CREATE|os.O_RDWR, 0644)

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

	if err := objectFS.MkdirAll(Path(signature), 0755); err != nil {
		log.Println(err)
		return nil, err
	}

	file, err := objectFS.Create(KeyPath("private", signature))

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

	file, err = objectFS.Create(KeyPath("public", signature))

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

	// Close the file to ensure the data is written
	err = file.Close()

	if err != nil {
		return nil, err
	}

	return key, nil
}

func GetPrivateKey(signature string, objectFS *storage.FileSystem) (*rsa.PrivateKey, error) {
	privateKeysMutex.Lock()
	defer privateKeysMutex.Unlock()

	if privateKeys[signature] == nil {
		privateKey, err := objectFS.ReadFile(KeyPath("private", signature))

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

func GetPublicKey(signature string, objectFS *storage.FileSystem) (*rsa.PublicKey, error) {
	path := KeyPath("public", signature)

	publicKey, err := objectFS.ReadFile(path)

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

func GetRawPublicKey(signature string, objectFS *storage.FileSystem) ([]byte, error) {
	path := KeyPath("public", signature)

	file, err := objectFS.ReadFile(path)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func KeyManagerInit(c *config.Config, secretsManager *SecretsManager) error {
	// Generate a public key for the signature if one does not exist
	err := generate(c, secretsManager.ObjectFS)

	if err != nil {
		return err
	}

	// Rotate the keys if a new signature is set
	rotate(c, secretsManager)

	return nil
}

func KeyPath(keyType string, signature string) string {
	return Path(signature) + fmt.Sprintf("%s.key", keyType)
}

func NextSignature(c *config.Config, secretsManager *SecretsManager, signature string) (string, error) {
	if c.Signature == signature {
		publickey, err := GetRawPublicKey(signature, secretsManager.ObjectFS)

		if err == nil {
			return EncryptKey(signature, string(publickey))
		}
	}

	c.SignatureNext = signature

	_, err := generatePrivateKey(signature, secretsManager.ObjectFS)

	if err != nil {
		return "", err
	}

	err = rotate(c, secretsManager)

	if err != nil {
		log.Println(err)
		return "", err
	}

	Broadcast("next_signature", signature)

	publicKey, err := GetRawPublicKey(signature, secretsManager.ObjectFS)

	if err != nil {
		return "", err
	}

	return EncryptKey(signature, string(publicKey))
}

func rotate(c *config.Config, secretsManager *SecretsManager) error {
	if c.SignatureNext == "" {
		return nil
	}

	if _, err := secretsManager.ObjectFS.Stat(Path(fmt.Sprintf("%s/%s", c.SignatureNext, ".rotate-lock"))); err == nil {
		return nil
	}

	if _, err := secretsManager.ObjectFS.Stat(Path(fmt.Sprintf("%s/%s", c.SignatureNext, "manifest.json"))); err == nil {
		return nil
	}

	// create rotate lock
	if err := secretsManager.ObjectFS.MkdirAll(Path(c.SignatureNext), 0755); err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.WriteFile(Path(fmt.Sprintf("%s/%s", c.SignatureNext, ".rotate-lock")), []byte{}, 0666); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var errors = []error{}

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := rotateAccessKeys(c, secretsManager)

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := rotateSettings(c, secretsManager)

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := rotateDatabaseKeys(c, secretsManager)

		if err != nil {
			errors = append(errors, err)
		}
	}()

	wg.Wait()

	for _, err := range errors {
		return err
	}

	manifest := map[string]interface{}{
		"signature":  c.Signature,
		"rotated_at": time.Now().Unix(),
	}

	manifestBytes, err := json.Marshal(manifest)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.WriteFile(Path(c.SignatureNext+"/manifest.json"), manifestBytes, 0666); err != nil {
		return err
	}

	if err := os.Remove(Path(fmt.Sprintf("%s/%s", c.SignatureNext, ".rotate-lock"))); err != nil {
		return err
	}

	return nil
}

func rotateAccessKeys(c *config.Config, secretsManager *SecretsManager) error {
	accessKeyDir := strings.Join([]string{
		Path(c.Signature),
		"access_keys",
	}, "/")

	newAccessKeyDir := strings.Join([]string{
		Path(c.SignatureNext),
		"access_keys/",
	}, "/")

	accessKeys, err := secretsManager.ObjectFS.ReadDir(accessKeyDir)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.MkdirAll(newAccessKeyDir, 0755); err != nil {
		return err
	}

	for _, accessKey := range accessKeys {
		accessKeyBytes, err := secretsManager.ObjectFS.ReadFile(strings.Join([]string{
			accessKeyDir,
			accessKey.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		decryptedAccessKey, err := secretsManager.Decrypt(c.Signature, string(accessKeyBytes))

		if err != nil {
			return err
		}

		encryptedAccessKey, err := secretsManager.Encrypt(c.SignatureNext, decryptedAccessKey.Value)

		if err != nil {
			return err
		}

		if err := secretsManager.ObjectFS.WriteFile(strings.Join([]string{
			newAccessKeyDir,
			accessKey.Name(),
		}, "/"), []byte(encryptedAccessKey), 0666); err != nil {
			return err
		}
	}

	return nil
}

func rotateDatabaseKeys(c *config.Config, secretsManager *SecretsManager) error {
	keysDir := strings.Join([]string{
		Path(c.Signature),
		"database_keys",
	}, "/")

	newKeysDir := strings.Join([]string{
		Path(c.SignatureNext),
		"database_keys/",
	}, "/")

	keys, err := secretsManager.ObjectFS.ReadDir(keysDir)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.MkdirAll(newKeysDir, 0755); err != nil {
		return err
	}

	for _, key := range keys {
		databaseKeyBytes, err := secretsManager.ObjectFS.ReadFile(strings.Join([]string{
			keysDir,
			key.Name(),
		}, "/"))

		if err != nil {
			return err
		}

		if err := secretsManager.ObjectFS.WriteFile(strings.Join([]string{
			newKeysDir,
			key.Name(),
		}, "/"), databaseKeyBytes, 0666); err != nil {
			return err
		}
	}

	return nil
}

func rotateSettings(c *config.Config, secretsManager *SecretsManager) error {
	var databaseSettings []internalStorage.DirEntry

	settingsDir := strings.Join([]string{
		Path(c.Signature),
		"settings",
	}, "/")

	newSettingsDir := strings.Join([]string{
		Path(c.SignatureNext),
		"settings/",
	}, "/")

	settings, err := secretsManager.ObjectFS.ReadDir(settingsDir)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.MkdirAll(newSettingsDir, 0755); err != nil {
		return err
	}

	for _, setting := range settings {
		if err := secretsManager.ObjectFS.MkdirAll(strings.Join([]string{
			newSettingsDir,
			setting.Name(),
		}, "/"), 0755); err != nil {
			return err
		}

		databaseSettings, err = secretsManager.ObjectFS.ReadDir(strings.Join([]string{
			settingsDir,
			setting.Name(),
			"/",
		}, "/"))

		if err != nil {
			return err
		}

		for _, databaseSetting := range databaseSettings {
			databaseSettingBytes, err := secretsManager.ObjectFS.ReadFile(strings.Join([]string{
				settingsDir,
				setting.Name(),
				databaseSetting.Name(),
			}, "/"))

			if err != nil {
				return err
			}

			decryptedSetting, err := secretsManager.Decrypt(c.Signature, string(databaseSettingBytes))

			if err != nil {
				return err
			}

			encryptedSetting, err := secretsManager.Encrypt(c.SignatureNext, decryptedSetting.Value)

			if err != nil {
				return err
			}

			if err := secretsManager.ObjectFS.WriteFile(strings.Join([]string{
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
