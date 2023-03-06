package storage

import (
	"sync"
	"net"
)

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

func (ms *ConnStorage) Get(key string) (net.Conn, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
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

func (ms *ConnStorage) Keys() []string {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	keys := make([]string, 0, ms.Length())
    for key := range ms.m {
        keys = append(keys, key)
    }

	return keys
}