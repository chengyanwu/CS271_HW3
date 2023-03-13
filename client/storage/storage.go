package storage

import (
	"sync"
	"net"
)

// Stores the last updated index on all other clients known by the LEADER client
type StringIntMap struct {
	mu sync.Mutex
	m map[string]int
}

type ConnStorage struct {
	mu sync.Mutex
	m  map[string]net.Conn
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