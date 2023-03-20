package dictionary

import (
	"crypto/rsa"
	"fmt"
)

// Dictionary object struct, part of the state machine storage
type Dictionary struct {
	DictId     string
	ClientIds  []string
	PrivateKey *rsa.PrivateKey   // nil if client doesn't have access to the dictionary
	PublicKey  *rsa.PublicKey
	Dict       map[string]string
}

func NewDict(id string, clientIDs []string, privKey *rsa.PrivateKey, pubKey *rsa.PublicKey) Dictionary {
	return Dictionary {
		DictId:    id,
		ClientIds: clientIDs,
		PublicKey: pubKey,
		PrivateKey: privKey,
		Dict:      make(map[string]string),
	}
}

func (d *Dictionary) Put(key string, value string) {
	d.Dict[key] = value
}

func (d *Dictionary) Get(key string) (string, bool) {
	value, exists := d.Dict[key]

	return value, exists
}

func (d *Dictionary) String() string {
	var ans string
	for key, val := range d.Dict {
		ans += fmt.Sprintf("Key: %s", key)
		ans += fmt.Sprintf("   Value: %s\n", val)
	}

	return ans
}

func (d *Dictionary) ClientIsMember(clientName string) bool {
	for _, value := range d.ClientIds {
		if clientName == value {
			return true
		}
	}

	return false
}
