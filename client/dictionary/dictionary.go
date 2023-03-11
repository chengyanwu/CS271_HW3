package dictionary

import (
	"strconv"
)

// dictionary object struct
type Dictionary struct {
	Dict_ID   string
	ClientIDs []string
	PublicKey string
	Dict      map[string]string
}

func (d *Dictionary) NewDict(id string, cnt int, clientIDs []string, publicKey string, dict map[string]string) Dictionary {
	return Dictionary{
		Dict_ID:   id + strconv.Itoa(cnt),
		ClientIDs: clientIDs,
		PublicKey: publicKey,
		Dict:      dict,
	}
}

func (d *Dictionary) Put(key string, value string) {
	d.Dict[key] = value
}

func (d *Dictionary) Get(key string) string {
	return d.Dict[key]
}
