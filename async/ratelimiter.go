package async

import (
	"context"
	"sync"
	"time"
	"fmt"
)

type RateLimiter struct {
	sem   *Semaphore
	max   int
	quota int
	mu    sync.Mutex

}

func NewRateLimiter(maxparallel, maxpersecond int, ctx context.Context) *RateLimiter {
	if maxparallel < 1 || maxpersecond < 1 {
		panic(fmt.Sprintf("NewRateLimiter given invalid args maxparallel: %s, maxpersecond: %s", maxparallel, maxpersecond))
	}
	r := &RateLimiter{
		sem: NewSemaphore(maxparallel, ctx),
		max: maxpersecond,
	}

	go func() {
		ticker := time.NewTicker(time.Second).C
		for {
			select {
			case <-ticker:
				r.mu.Lock()
				r.quota = 0
				r.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	return r
}

func (r *RateLimiter) Acquire() {

}

func (r *RateLimiter) Release() {
	r.sem.Release()

}
