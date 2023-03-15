package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// Will contain helper functions to generate public and private keys
type Keys struct {
	Public             string
	Private            *rsa.PrivateKey
	DictionaryPubKeys  map[string]string
	DictionaryPrivKeys map[string]string
}

func NewKeys() Keys {
	// generate public and private keys
	key, err := rsa.GenerateKey(rand.Reader, 256)
	if err != nil {
		panic(err)
	}

	// Encode the public key in PEM format
	pemEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey),
	})

	return Keys{
		Public:             string(pemEncoded),
		Private:            key,
		DictionaryPrivKeys: make(map[string]string),
		DictionaryPubKeys:  make(map[string]string),
	}
}

func (k *Keys) Print() {
	fmt.Println("Public key:", k.Public)
	fmt.Println("Private key:", k.Private)
}
