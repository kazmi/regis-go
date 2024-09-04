package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type InMemoryKeyValueStore struct {
	mu     sync.RWMutex
	store  map[string]string
	expiry map[string]time.Time
}

func (i *InMemoryKeyValueStore) Set(key, value string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.store[key] = value
}

func (i *InMemoryKeyValueStore) SetWithExpiry(key, value, expiry string) {
	i.Set(key, value)
	expiresAt, err := strconv.Atoi(expiry)
	if err != nil {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.expiry[key] = time.Now().Add(time.Duration(expiresAt) * time.Millisecond)
	}
}

func (i *InMemoryKeyValueStore) Get(key string) (string, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	value, found := i.store[key]

	expiresAt, has_expiry := i.expiry[key]
	if has_expiry && time.Now().Before(expiresAt) {
		return value, true
	} else {
		delete(i.store, key)
		delete(i.expiry, key)
	}

	return value, found

}

func (i *InMemoryKeyValueStore) Dump() {
	for k, v := range i.expiry {
		fmt.Println("k: ", k, "v: ", v)
	}
}
