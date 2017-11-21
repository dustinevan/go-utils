package async

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mediaFORGE/supplyqc/infra/cache"
)

var CHANFULL = errors.New("the channel is full, nothing can be inserted until consumers catch up")
var UNIQUEUE_CLOSED = errors.New("this uniqueue is closed")

type UniQueue struct {
	// name is for logging purposes only
	name string

	// mutex used to lock around  cache read/write/delete operations
	mu sync.RWMutex
	// the amount of a key passed to Insert is cached
	// and thus deduped in the output channel.
	deduptime time.Duration
	// track how many records are in the cache, doesn't need but
	// these it currently only changes inside locks
	records int
	// max size of the dedup cache
	maxdedup int
	dedup    map[string]int64

	// track how many records are inflight, i.e deduped records in the channel
	// this is used to keep the Insert func from blocking. Insert returns CHANFULL
	// when inflight == maxinflight
	inflight    int32
	maxinflight int
	q           chan string

	// in order to keep track of inflight, a go routine reads from q and writes to outgoing
	// this way reads from in the inflight chan (q) are known. This chan is set to 8, so
	// the actual number of strings in the UniQueue is inflight + 8
	outgoing chan string

	// when done the cache cleanup go routine closes the channels, drains them, amnd returns
	ctx    context.Context
	closed int32
}

func NewUniQueue(name string, maxdedup, maxinflight int, deduptime time.Duration, parentCtx context.Context) *UniQueue {

	t := &UniQueue{
		name:        name,
		deduptime:   deduptime,
		maxdedup:    maxdedup,
		dedup:       make(map[string]int64),
		maxinflight: maxinflight,
		q:           make(chan string, maxinflight),
		outgoing:    make(chan string, 8),
		ctx:         parentCtx,
	}

	// cache cleanup go routine. if ctx.Err, this closes and drains the channels
	go func() {
		ticker := time.NewTicker(deduptime)
		for {
			select {
			case <-ticker.C:
				now := time.Now().Unix()
				t.mu.Lock()
				for k, v := range t.dedup {
					if v < now {
						delete(t.dedup, k)
						t.records--
					}
				}
				t.mu.Unlock()
			case <-t.ctx.Done():
				return
			}
		}
	}()

	// transfer from q to outgoing. this is how we keep track of inflight
	// which makes Insert() non-blocking
	go func() {
		defer close(t.outgoing)
		for s := range t.q {
			t.outgoing <- s
			atomic.AddInt32(&t.inflight, -1)
		}
	}()

	// shutdown goroutine
	go func() {
		<-t.ctx.Done()
		t.mu.Lock()
		t.closed = 1
		close(t.q)
		drainString(t.q)
		drainString(t.outgoing)
		t.mu.Unlock()
	}()

	TickerLog(time.Second*30, parentCtx, func() {
		log.Printf("%s UniQueue has %v records inflight", t.name, atomic.LoadInt32(&t.inflight))
	})

	return t
}

// Checks if the value exists in the cache
// most callers should call insert and read the error
func (c *UniQueue) Check(s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.dedup[s]
	return ok
}

// Insert attempts to push another string to the channel. checks for existence, chan and cache sizes
// before writing to the internal inflight channel.
func (c *UniQueue) Insert(s string) error {
	// ok with race conditions here
	if c.closed == 1 {
		return UNIQUEUE_CLOSED
	}

	_, ok := c.dedup[s]
	if ok {
		return cache.ALREADY_EXISTS
	}

	if c.records == c.maxdedup {
		return cache.CACHEFULL
	}

	// not ok with race conditions here
	c.mu.Lock()
	defer c.mu.Unlock()
	// check again inside the lock.
	if c.closed == 1 {
		return UNIQUEUE_CLOSED
	}

	if atomic.LoadInt32(&c.inflight) == int32(c.maxinflight) {
		return CHANFULL
	}

	c.dedup[s] = time.Now().Add(c.deduptime).Unix()
	c.records++

	// because of the check above this shouldn't block
	atomic.AddInt32(&c.inflight, 1)
	c.q <- s

	return nil
}

// Uncaches a specific dedup cache entry, which unblocks it from flowing through the channel
func (c *UniQueue) UnCache(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.dedup, s)
	c.records--
}

// GetChan returns the output channel, used by n consumers to consume deduped records.
func (c *UniQueue) GetChan() <-chan string {
	if atomic.LoadInt32(&c.closed) == 1 {
		panic("Cache Channel is closed")
	}
	return c.outgoing
}
