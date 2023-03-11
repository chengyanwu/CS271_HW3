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

func (d *Dictionary) New(id string, cnt int, clientIDs []string, publicKey string, dict map[string]string) {
	d.Dict_ID = id + strconv.Itoa(cnt)
	d.ClientIDs = clientIDs
	d.PublicKey = publicKey
	d.Dict = dict
}

func (d *Dictionary) put(key string, value string) {
	d.Dict[key] = value
}

func (d *Dictionary) get(key string) string {
	return d.Dict[key]
}
