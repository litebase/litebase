package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
	"golang.org/x/crypto/hkdf"
)

var privateKeys = map[string]*rsa.PrivateKey{}
var privateKeysMutex = &sync.Mutex{}

// Decrypt a private key using AES-GCM with HKDF for key derivation.
func decryptPrivateKey(key string, encryptedData []byte) ([]byte, error) {
	hash := sha256.New()
	saltSize := hash.Size()

	// Minimum size check: salt + nonce + ciphertext (at least 1 byte) + auth tag (16 bytes)
	if len(encryptedData) < saltSize+12+1+16 {
		return nil, fmt.Errorf("encrypted data too short")
	}

	// Extract salt from the beginning
	salt := encryptedData[:saltSize]

	// Derive the same key using HKDF
	secret := sha256.Sum256([]byte(key))
	info := []byte("litebase data encryption key")

	hkdf := hkdf.New(sha256.New, secret[:], salt, info)

	keySlice := make([]byte, 32)

	if _, err := io.ReadFull(hkdf, keySlice); err != nil {
		return nil, fmt.Errorf("failed to read full key from HKDF: %w", err)
	}

	// Create cipher
	block, err := aes.NewCipher(keySlice)

	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := aead.NonceSize()

	// Check we have enough data for nonce + ciphertext
	if len(encryptedData) < saltSize+nonceSize {
		return nil, fmt.Errorf("encrypted data too short for nonce")
	}

	// Extract nonce and ciphertext
	nonce := encryptedData[saltSize : saltSize+nonceSize]
	ciphertext := encryptedData[saltSize+nonceSize:]

	// Decrypt and verify
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// Encrypt a private key using AES-GCM with HKDF for key derivation.
func encryptPrivateKey(key string, privateKey []byte) ([]byte, error) {
	hash := sha256.New()

	salt := make([]byte, hash.Size())

	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	secret := sha256.Sum256([]byte(key))
	info := []byte("litebase data encryption key")

	hkdf := hkdf.New(sha256.New, secret[:], salt, info)

	keySlice := make([]byte, 32)

	if _, err := io.ReadFull(hkdf, keySlice); err != nil {
		return nil, fmt.Errorf("failed to read full key from HKDF: %w", err)
	}

	block, err := aes.NewCipher(keySlice)

	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, privateKey, nil)

	// Format: salt + nonce + ciphertext (includes auth tag)
	result := make([]byte, 0, len(salt)+len(nonce)+len(ciphertext))
	result = append(result, salt...)
	result = append(result, nonce...)
	result = append(result, ciphertext...)

	return result, nil
}

// Generate a new key for the current encryption key if one does not exist.
func generate(c *config.Config, objectFS *storage.FileSystem) error {
	encryptionKey := c.EncryptionKey

	// Ensure the encryption key is a 32 byte hash
	if len(encryptionKey) != 64 {
		return errors.New("invalid encryption key length")
	}

	_, err := objectFS.Stat(KeyPath("private", encryptionKey))

	if os.IsNotExist(err) {
		_, err := generatePrivateKey(encryptionKey, objectFS)

		if err != nil {
			return err
		}
	}

	return nil
}

func generatePrivateKey(encryptionKey string, objectFS *storage.FileSystem) (*rsa.PrivateKey, error) {
	var err error

	if _, err := objectFS.Stat(KeyPath("private", encryptionKey)); err == nil {
		return nil, errors.New("private key already exists")
	}

	// Create the encryption directory if it does not exist
	encryptionDirectory := Path(encryptionKey)

	if _, err := objectFS.Stat(encryptionDirectory); os.IsNotExist(err) {
		if err := objectFS.MkdirAll(encryptionDirectory, 0750); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	key, err := rsa.GenerateKey(rand.Reader, 3072)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := objectFS.MkdirAll(Path(encryptionKey), 0750); err != nil {
		log.Println(err)
		return nil, err
	}

	// Write the key to the file
	fileData := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	encryptedFileData, err := encryptPrivateKey(encryptionKey, fileData)

	if err != nil {
		slog.Error("Failed to encrypt private key", "error", err)
		return nil, err
	}

	if err := objectFS.WriteFile(KeyPath("private", encryptionKey), encryptedFileData, 0600); err != nil {
		slog.Error("Failed to write private key", "error", err)
		return nil, err
	}

	return key, nil
}

// GetPrivateKey retrieves the private key for the given encryption key.
func GetPrivateKey(encryptionKey string, objectFS *storage.FileSystem) (*rsa.PrivateKey, error) {
	privateKeysMutex.Lock()
	defer privateKeysMutex.Unlock()

	if privateKeys[encryptionKey] == nil {
		encryptedPrivateKey, err := objectFS.ReadFile(KeyPath("private", encryptionKey))

		if err != nil {
			slog.Debug("Failed to read private key", "error", err.Error())
			return nil, err
		}

		// Decrypt the private key
		privateKeyData, err := decryptPrivateKey(encryptionKey, encryptedPrivateKey)

		if err != nil {
			slog.Error("Failed to decrypt private key", "error", err)
			return nil, err
		}

		block, _ := pem.Decode(privateKeyData)

		if block == nil {
			return nil, errors.New("failed to parse PEM block containing the key")
		}

		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)

		if err != nil {
			return nil, err
		}

		privateKeys[encryptionKey] = key
	}

	return privateKeys[encryptionKey], nil
}

func HasKey(encryptionKey string, objectFS *storage.FileSystem) bool {
	_, err := objectFS.Stat(Path(encryptionKey))

	if os.IsNotExist(err) {
		return false
	}

	if err != nil {
		slog.Error("Error checking key existence", "error", err)
		return false
	}

	return true
}

// Initialize the key manager by generating a private key for the encryption key if
// one does not exist.
func KeyManagerInit(c *config.Config, secretsManager *SecretsManager) error {
	// Generate a private key for the encryption key if one does not exist
	err := generate(c, secretsManager.ObjectFS)

	if err != nil {
		return err
	}

	return nil
}

// Return the path for a key for the given encryption key.
func KeyPath(keyType string, encryptionKey string) string {
	return Path(encryptionKey) + fmt.Sprintf("%s.key", keyType)
}

// Initialize the next encryption key.
func NextEncryptionKey(auth *Auth, c *config.Config, encryptionKey string) error {
	if c.EncryptionKey == encryptionKey {
		return errors.New("the encryption key is already the current encryption key")
	}

	c.EncryptionKeyNext = encryptionKey

	_, err := generatePrivateKey(encryptionKey, auth.SecretsManager.ObjectFS)

	if err != nil {
		return err
	}

	err = rotate(c, auth.SecretsManager)

	if err != nil {
		log.Println(err)
		return err
	}

	auth.Broadcast("next_encryption_key", encryptionKey)

	return nil
}

// Rotate the secrets for the next encryption key.
func rotate(c *config.Config, secretsManager *SecretsManager) error {
	if c.EncryptionKeyNext == "" {
		return nil
	}

	if _, err := secretsManager.ObjectFS.Stat(Path(c.EncryptionKeyNext) + ".rotate-lock"); err == nil {
		return nil
	}

	if _, err := secretsManager.ObjectFS.Stat(Path(c.EncryptionKeyNext) + "manifest.json"); err == nil {
		return nil
	}

	// create rotate lock
	if err := secretsManager.ObjectFS.MkdirAll(Path(c.EncryptionKeyNext), 0750); err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.WriteFile(Path(c.EncryptionKeyNext)+".rotate-lock", []byte{}, 0600); err != nil {
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

	manifest := map[string]any{
		"encryption_key": c.EncryptionKeyNext,
		"rotated_at":     time.Now().UTC().Unix(),
	}

	manifestBytes, err := json.Marshal(manifest)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.WriteFile(Path(c.EncryptionKeyNext)+"manifest.json", manifestBytes, 0600); err != nil {

		return err
	}

	if err := secretsManager.ObjectFS.Remove(Path(c.EncryptionKeyNext) + ".rotate-lock"); err != nil {
		return err
	}

	return nil
}

func rotateAccessKeys(c *config.Config, secretsManager *SecretsManager) error {
	accessKeyDir := Path(c.EncryptionKey) + "access_keys/"
	newAccessKeyDir := Path(c.EncryptionKeyNext) + "access_keys/"

	accessKeys, err := secretsManager.ObjectFS.ReadDir(accessKeyDir)

	if err != nil {
		return err
	}

	if err := secretsManager.ObjectFS.MkdirAll(newAccessKeyDir, 0750); err != nil {
		return err
	}

	for _, accessKey := range accessKeys {
		accessKeyBytes, err := secretsManager.ObjectFS.ReadFile(
			accessKeyDir + accessKey.Name(),
		)

		if err != nil {
			return err
		}

		decryptedAccessKey, err := secretsManager.Decrypt(c.EncryptionKey, accessKeyBytes)

		if err != nil {
			return err
		}

		encryptedAccessKey, err := secretsManager.Encrypt(c.EncryptionKeyNext, []byte(decryptedAccessKey.Value))

		if err != nil {
			return err
		}

		if err := secretsManager.ObjectFS.WriteFile(
			newAccessKeyDir+accessKey.Name(),
			[]byte(encryptedAccessKey),
			0600,
		); err != nil {
			return err
		}
	}

	return nil
}

func rotateDatabaseKeys(c *config.Config, secretsManager *SecretsManager) error {
	currentDks, err := secretsManager.DatabaseKeyStore(c.EncryptionKey)

	if err != nil {
		return err
	}

	newDks, err := secretsManager.DatabaseKeyStore(c.EncryptionKeyNext)

	if err != nil {
		return err
	}

	for databaseKey := range currentDks.All() {
		err := newDks.Put(databaseKey)

		if err != nil {
			slog.Error("Failed to put database key:", "error", err)
			return err
		}
	}

	return nil
}

func rotateSettings(c *config.Config, secretsManager *SecretsManager) error {
	var databaseSettings []internalStorage.DirEntry

	settingsDir := Path(c.EncryptionKey) + "settings/"
	newSettingsDir := Path(c.EncryptionKeyNext) + "settings/"

	settings, err := secretsManager.ObjectFS.ReadDir(settingsDir)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	if err := secretsManager.ObjectFS.MkdirAll(newSettingsDir, 0750); err != nil {
		return err
	}

	for _, setting := range settings {
		if err := secretsManager.ObjectFS.MkdirAll(
			newSettingsDir+setting.Name()+"/",
			0750,
		); err != nil {
			return err
		}

		databaseSettings, err = secretsManager.ObjectFS.ReadDir(
			settingsDir + setting.Name() + "/",
		)

		if err != nil {
			return err
		}

		for _, databaseSetting := range databaseSettings {
			databaseSettingBytes, err := secretsManager.ObjectFS.ReadFile(
				settingsDir + setting.Name() + "/" + databaseSetting.Name(),
			)

			if err != nil {
				return err
			}

			decryptedSetting, err := secretsManager.Decrypt(c.EncryptionKey, databaseSettingBytes)

			if err != nil {
				return err
			}

			encryptedSetting, err := secretsManager.Encrypt(c.EncryptionKeyNext, []byte(decryptedSetting.Value))

			if err != nil {
				return err
			}

			if err := secretsManager.ObjectFS.WriteFile(
				newSettingsDir+setting.Name()+"/"+databaseSetting.Name(),
				[]byte(encryptedSetting),
				0600,
			); err != nil {
				return err
			}
		}
	}

	return nil
}
