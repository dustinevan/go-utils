package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type CollectFn func(b []byte) error

type CollectOption func(c *Collect)

type Collect struct {
	fn CollectFn

	ctx    context.Context
	canc   context.CancelFunc
	donewg sync.WaitGroup

	errs  chan error
	stats chan string
}

func NewCollect(fn CollectFn, in <-chan [][]byte, opts ...CollectOption) *Collect {
	ctx, canc := context.WithCancel(context.Background())
	c := &Collect{
		fn: fn,

		ctx:  ctx,
		canc: canc,

		errs:  make(chan error, 10000),
		stats: make(chan string, 8),
	}
	for _, opt := range opts {
		opt(c)
	}

	c.donewg.Add(1)
	go func() {
		defer c.donewg.Done()
		c.collect(in)
	}()

	return c
}

func (c *Collect) collect(in <-chan [][]byte) {
	success := 0
	failed := 0
	start := time.Now()
	for chunk := range in {

		select {
		case <-c.ctx.Done():
			c.submitErr(fmt.Errorf("collectstream: canceled"))
			continue
		default:
			for _, bytes := range chunk {
				err := c.fn(bytes)
				if err != nil {
					c.submitErr(fmt.Errorf("collect: encountered error: %s on record: %s, skipping", err, string(bytes)))
					failed++
					continue
				}
				success++
			}
		}
	}
	c.submitStat(fmt.Sprintf("successful collect of %v messages; %v failed; in %s",
		success, failed, time.Since(start)))
}

func (c *Collect) submitStat(s string) {
	select {
	case c.stats <- s:
	default:
	}
}

func (c *Collect) submitErr(e error) {
	select {
	case c.errs <- e:
	default:
	}
}

func (c *Collect) ListenErr() <-chan error {
	return c.errs
}

func (c *Collect) ListenStats() <-chan string {
	return c.stats
}

func (c *Collect) Wait() {
	c.donewg.Wait()
}

func (c *Collect) Cancel() {
	c.canc()
}
