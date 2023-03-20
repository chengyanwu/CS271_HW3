package storage

import (
	"sync"
	"net"
	"example/users/client/dictionary"
	"crypto/rsa"
)

// Stores the last updated index on all other clients known by the LEADER client
type StringIntMap struct {
	mu sync.Mutex
	m map[string]int
}

type DictofDicts struct {
	mu sync.Mutex
	m map[string]*dictionary.Dictionary // dictionary_id
}

type ConnStorage struct {
	mu sync.Mutex
	m  map[string]net.Conn
}

type Faillinks struct {
	mu sync.Mutex
	m  map[string]bool
}

func NewDictofDicts() DictofDicts {
	m := make(map[string]*dictionary.Dictionary)
	return DictofDicts{
		m: m,
	}
}

// Creates a new dictionary in the state machine!
func (d *DictofDicts) NewDict(dictionary_id string, clientIDs []string, privKey *rsa.PrivateKey, pubKey *rsa.PublicKey) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.m[dictionary_id]; !exists {
		newDict := dictionary.NewDict(dictionary_id, clientIDs, privKey, pubKey)
		d.m[dictionary_id] = &newDict

		return true // new dictionary is created
	}

	return false // SHOULD NEVER happen: dictionary_id already exists
}

func (d *DictofDicts) Put(dictionary_id, key, value string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		d.m[dictionary_id].Put(key, value)
		
		return true
	}

	return false // SHOULD NEVER happen: dictionary_id doesn't exist
}

func (d *DictofDicts) Exists(dictionary_id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, exists := d.m[dictionary_id]

	return exists
}

func (d *DictofDicts) Get(dictionary_id, key string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		ans, succ := d.m[dictionary_id].Get(key)

		return ans, succ
	}

	return "", false // fails if key isn't present in the dictionary or if dictionary_id doesn't exists
}

func (d *DictofDicts) GetPubKey(dictionary_id string) (*rsa.PublicKey, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		ans := d.m[dictionary_id].PublicKey

		return ans, true
	}

	return nil, false
}

func (d *DictofDicts) GetPrivKey(dictionary_id string) (*rsa.PrivateKey, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		ans := d.m[dictionary_id].PrivateKey

		return ans, true
	}
	
	return nil, false
}

func (d *DictofDicts) GetClientIds(dictionary_id string) ([]string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		ans := d.m[dictionary_id].ClientIds

		return ans, true
	}

	return nil, false
}

func (d *DictofDicts) PrintDict(dictionary_id string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.m[dictionary_id]; exists {
		ans := d.m[dictionary_id].String()

		return ans, true
	}

	return "", false
}

// Gets the member dictionaries of the client
func (d *DictofDicts) GetMemberDictionaries(clientName string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	var ids []string
	for _, dict := range d.m {
		if dict.ClientIsMember(clientName) {
			ids = append(ids, dict.DictId)
		}
	}

	return ids
}

func NewConnStorage() ConnStorage {
	m := make(map[string]net.Conn)
	return ConnStorage{
		m: m,
	}
}

func NewStringIntMap() StringIntMap {
	m := make(map[string]int)
	return StringIntMap{
		m: m,
	}
}

func (ms *StringIntMap) Get(key string) (int, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]

	return v, found
}

func (ms *StringIntMap) Set(key string, value int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *ConnStorage) Get(key string) (net.Conn, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]

	// if found && v == nil {
		// found = false
	// }

	return v, found
}

func (ms *ConnStorage) Set(key string, value net.Conn) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *ConnStorage) Length() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m)
}

// Retrieves the keys from the store that have non-nil connections
func (ms *ConnStorage) Keys(all bool) []string {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	keys := make([]string, 0, len(ms.m))
    for key := range ms.m {
		if all || ms.m[key] != nil {
       		keys = append(keys, key)
		}
    }

	return keys
}

func (f *Faillinks) Init() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m = make(map[string]bool)
	f.m["A"] = false	
	f.m["B"] = false
	f.m["C"] = false
	f.m["D"] = false
	f.m["E"] = false
}

func (f *Faillinks) Set(key string, value bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
}

func (f *Faillinks) Get(key string) (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, found := f.m[key]

	return v, found
}