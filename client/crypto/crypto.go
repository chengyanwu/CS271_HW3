package crypto

// Helper library that implements cryptography operations used for this project

import (
	"encoding/pem"
	"fmt"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/sha256"
)

// Will contain helper functions to generate public and private keys
type Keys struct {
	Public             *rsa.PublicKey
	Private            *rsa.PrivateKey
}

func NewKeys() Keys {
	// generate public and private keys
	privKey, err := rsa.GenerateKey(rand.Reader, 256)
	if err != nil {
		panic(err.Error())
	}

	pubKey := &privKey.PublicKey

	// Encode the public key in PEM format
	// pemEncoded := pem.EncodeToMemory(&pem.Block{
	// 	Type:  "RSA PUBLIC KEY",
	// 	Bytes: x509.MarshalPKCS1PublicKey(pubKey),
	// })

	return Keys{
		Public: pubKey,
		Private: privKey,
	}
}

func Encrypt(target []byte, key *rsa.PublicKey) []byte {
	result, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, key, target, nil)

	if err != nil {
		panic("Failed to encrypt target")
	}

	return result
}

// Return a set of keys for this process from byte slice
func NewKeysFromExistingBytes(privByte []byte) Keys {
	privBlock, _ := pem.Decode(privByte)
	privKey, err := x509.ParsePKCS1PrivateKey(privBlock.Bytes)

	if err != nil {
		panic("Something went wrong when parsing bytes into a private key")
	}

	return Keys{
		Public: &privKey.PublicKey,
		Private: privKey,
	}
}

// demarshal
func (k *Keys) SetKeys(privByte []byte) {
	privBlock, _ := pem.Decode(privByte)
	privKey, err := x509.ParsePKCS1PrivateKey(privBlock.Bytes)

	if err != nil {
		panic("Something went wrong when parsing bytes into a private key")
	}

	k.Public = &privKey.PublicKey
	k.Private = privKey
}

// Converts bytes to a public key
func ByteToPubKey(b []byte) (*rsa.PublicKey) {
	pub, _ := pem.Decode(b)
	pubKey, err := x509.ParsePKCS1PublicKey(pub.Bytes)
	
	if err != nil {
		panic("Something went wrong when parsing bytes into a public key")
	}

	return pubKey
}

// marshal
func (k *Keys) PubKeyToByte() []byte {
	pemEncoded := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(k.Public),
	})

	if pemEncoded == nil {
		panic("Something went wrong when encoding the public key to a byte slice")
	}

	return pemEncoded
}

func (k *Keys) PrivKeyToByte() []byte {
	pemEncoded := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(k.Private),
	})

	if pemEncoded == nil {
		panic("Something went wrong when encoding the private key to a byte slice")
	}

	return pemEncoded
}

func (k *Keys) String() string {
	return fmt.Sprintf("======= KEYS =======\nPublic Key: %v\nPrivate Key: %v\n====================\n", *k.Public, *k.Private)
}
