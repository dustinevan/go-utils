package cache

import (
	"sync"
	"time"

	"github.com/mediaFORGE/supplyqc/vendor/github.com/pkg/errors"
)

var CACHEFULL = errors.New("could not insert, the cache is full")
var ALREADY_EXISTS = errors.New("this key already exists")
var EMPTY_RECORD = errors.New("the value for this key is empty")


type TimeoutCache struct {
	c          map[string]time.Time
	records    int
	maxrecords int
	mu         sync.RWMutex
	scanRate time.Duration
}

func NewTimeoutCache(maxrecords int, opts ...CacheOption) *TimeoutCache {
	if maxrecords < 1 {
		maxrecords = 1
	}

	t := &TimeoutCache{
		c:          make(map[string]time.Time),
		records:    0,
		maxrecords: maxrecords,
		scanRate: time.Second * 30,
	}

	for _, opt := range opts {
		opt(t)
	}

	go func() {
		ticker := time.NewTicker(t.scanRate)
		for range ticker.C {
			t.mu.Lock()
			for k, v := range t.c {
				if v.Before(time.Now()) {
					delete(t.c, k)
					t.records--
				}
			}
			t.mu.Unlock()
		}
	}()

	return t
}

func (t *TimeoutCache) SetScanRate(duration time.Duration) {
	t.scanRate = duration
}


func (t *TimeoutCache) Check(k string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.c[k]
	return ok
}

func (t *TimeoutCache) Insert(s string, duration time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.c) >= t.maxrecords {
		return CACHEFULL
	}
	expiration := time.Now().Add(duration)

	_, ok := t.c[s]
	if ok {
		return ALREADY_EXISTS
	}
	t.c[s] = expiration
	t.records++
	return nil
}

func (t *TimeoutCache) Uncache(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.c, s)
	t.records--
}

func (t *TimeoutCache) UncacheMany(s []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range s {
		delete(t.c, k)
		t.records--
	}

}
