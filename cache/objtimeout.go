package cache

import (
	"sync"
	"time"
)

type WithOptions interface {
	SetScanRate(duration time.Duration)
}

type CacheOption func(cache WithOptions)

func ScanRate(duration time.Duration) CacheOption {
	return func(cache WithOptions) {
		cache.SetScanRate(duration)
	}
}

type val struct {
	t time.Time
	v interface{}
}

type ObjCache struct {
	c          map[string]*val
	records    int
	maxrecords int
	mu         sync.RWMutex
	scanRate   time.Duration
}

func NewObjCache(maxrecords int, opts ...CacheOption) *ObjCache {
	if maxrecords < 1 {
		maxrecords = 1
	}

	t := &ObjCache{
		c:          make(map[string]*val),
		records:    0,
		maxrecords: maxrecords,
		scanRate:   time.Second * 30,
	}

	for _, opt := range opts {
		opt(t)
	}

	go func() {
		ticker := time.NewTicker(t.scanRate)
		for range ticker.C {
			t.mu.Lock()
			for k, v := range t.c {
				if v.t.Before(time.Now()) {
					delete(t.c, k)
					t.records--
				}
			}
			t.mu.Unlock()
		}
	}()
	return t
}

func (h *ObjCache) SetScanRate(duration time.Duration) {
	h.scanRate = duration
}

func (h *ObjCache) Check(k string) bool {
	h.mu.Lock()
	_, ok := h.c[k]
	h.mu.Unlock()
	return ok
}

func (h *ObjCache) Get(k string) (interface{}, bool) {
	h.mu.Lock()
	v, ok := h.c[k]
	h.mu.Unlock()
	if v != nil {
		return v.v, ok
	}
	return nil, ok
}

func (h *ObjCache) Insert(s string, v interface{}, duration time.Duration) error {
	expiration := time.Now().Add(duration)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.c) >= h.maxrecords {
		return CACHEFULL
	}
	h.c[s] = &val{t: expiration, v: v}
	return nil
}

func (h ObjCache) Uncache(s string) {
	h.mu.Lock()
	delete(h.c, s)
	h.mu.Unlock()
	h.records--
}

func (h ObjCache) UncacheMany(s []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range s {
		delete(h.c, k)
		h.records--
	}
}
