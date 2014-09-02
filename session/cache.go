package session

import (
	"sync"
	"time"
)

type Cache struct {
	data            map[string][]byte
	dataLastUpdated map[string]time.Time
	mutex           sync.RWMutex
}

func NewCache() *Cache {
	cache := &Cache{}
	cache.data = make(map[string][]byte)
	cache.dataLastUpdated = make(map[string]time.Time)
	return cache
}

func (c *Cache) Put(command string, value []byte) {

	// write lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data[command] = value
	c.dataLastUpdated[command] = time.Now()
}

func (c *Cache) Get(command string) []byte {

	secondAgo := time.Now().Add(-time.Second)

	// read lock
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if _, ok := c.dataLastUpdated[command]; !ok || c.dataLastUpdated[command].Before(secondAgo) {
		// no data or too old
		return nil
	}

	return c.data[command]
}

func (c *Cache) cleanupCache() {

	oneSecondAgo := time.Now().Add(-time.Second)

	// write lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// we delete all keys that have expired
	for command, lastUpdated := range c.dataLastUpdated {

		if lastUpdated.Before(oneSecondAgo) {
			delete(c.data, command)
			delete(c.dataLastUpdated, command)
		}
	}

	// are we over the cache limit?
}
