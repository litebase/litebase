package auth

import (
	"testing"
)

func TestDecrypt(t *testing.T) {
	encrypter := NewEncrypter([]byte("secret"))

	encrypted, err := encrypter.Encrypt("hello")

	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := encrypter.Decrypt(encrypted)

	if err != nil {
		t.Fatal(err)
	}

	if decrypted.Value != "hello" {
		t.Fatal("Decrypted value does not match")
	}
}
