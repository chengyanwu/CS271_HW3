package crypto

// Helper library that implements cryptography operations used for this project

import (
	"encoding/pem"
	"fmt"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/sha256"
	"hash"
	"io"
)

// Will contain helper functions to generate public and private keys
type Keys struct {
	Public             *rsa.PublicKey
	Private            *rsa.PrivateKey
}

func NewKeys() Keys {
	// generate public and private keys
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
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
	result, err := EncryptOAEP(sha256.New(), rand.Reader, key, target, nil)

	if err != nil {
		panic(fmt.Sprintf("Failed to encrypt target: %s, length: %d, length of key: %d", err.Error(), len(target), key.Size()))
	}

	return result
}

func Decrypt(target []byte, key *rsa.PrivateKey) []byte {
	decrypted, err := DecryptOAEP(sha256.New(), rand.Reader, key, target, nil)

	if err != nil {
		panic(fmt.Sprintf("Failed to decrypt target: %s", err.Error()))
	}

	return decrypted
}

// Wrapper function that iteratively encrypts chunks of the target byte with the public key
func EncryptOAEP(hash hash.Hash, random io.Reader, public *rsa.PublicKey, target []byte, label []byte) ([]byte, error) {
    targetLen := len(target)
    step := public.Size() - 2 * hash.Size() - 2
    var encryptedBytes []byte

    for start := 0; start < targetLen; start += step {
        finish := start + step
        if finish > targetLen {
            finish = targetLen
        }

        encryptedBlockBytes, err := rsa.EncryptOAEP(hash, random, public, target[start:finish], label)
        if err != nil {
            return nil, err
        }

        encryptedBytes = append(encryptedBytes, encryptedBlockBytes...)
    }

    return encryptedBytes, nil
}

// wrapper function around DecryptOAEP that iteratively decrypts chunks of the encrypted message with the private key
func DecryptOAEP(hash hash.Hash, random io.Reader, private *rsa.PrivateKey, encrypted []byte, label []byte) ([]byte, error) {
    encryptedLen := len(encrypted)
    step := private.PublicKey.Size()
    var decryptedBytes []byte

    for start := 0; start < encryptedLen; start += step {
        finish := start + step
        if finish > encryptedLen {
            finish = encryptedLen
        }

        decryptedBlockBytes, err := rsa.DecryptOAEP(hash, random, private, encrypted[start:finish], label)
        if err != nil {
            return nil, err
        }

        decryptedBytes = append(decryptedBytes, decryptedBlockBytes...)
    }

    return decryptedBytes, nil
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

func ByteToPrivKey(b []byte) (*rsa.PrivateKey) {
	priv, _ := pem.Decode(b)
	privKey, err := x509.ParsePKCS1PrivateKey(priv.Bytes)

	if err != nil {
		panic("Something went wrong when parsing bytes into a private key")
	}

	return privKey
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
