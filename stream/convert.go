package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ConvertFn func(b []byte) ([]byte, error)

type ConvertOption func(c *Convert)

type Convert struct {
	fn ConvertFn

	concurrency int
	outgoing    chan [][]byte

	ctx    context.Context
	canc   context.CancelFunc
	donewg sync.WaitGroup

	errs  chan error
	stats chan string
}

func NewConvert(fn ConvertFn, in <-chan [][]byte, opts ...ConvertOption) *Convert {
	ctx, canc := context.WithCancel(context.Background())
	c := &Convert{
		fn: fn,

		concurrency: 1,
		outgoing:    make(chan [][]byte, 16),

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
		defer close(c.outgoing)
		defer c.donewg.Done()
		c.convert(in)
	}()

	return c
}

func (c *Convert) convert(in <-chan [][]byte) {
	success := 0
	failed := 0
	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range in {
				select {
				case <-c.ctx.Done():
					c.submitErr(fmt.Errorf("convertstream: canceled"))
					continue
				default:
					bufs := make([][]byte, len(chunk))
					j := 0
					for _, bytes := range chunk {
						buf, err := c.fn(bytes)
						if err != nil {
							c.submitErr(fmt.Errorf("converter: encountered error: %s on record: %s, skipping", err, string(bytes)))
							failed++
							continue
						}
						success++
						bufs[j] = buf
						j++
					}
					success += j
					c.outgoing <- bufs[:j]
				}
			}
		}()
	}
	wg.Wait()
	c.submitStat(fmt.Sprintf("successful convert of %v messages; %v messages failed; finished in %s",
		success, failed, time.Since(start)))
}

func (c *Convert) submitStat(s string) {
	select {
	case c.stats <- s:
	default:
	}
}

func (c *Convert) submitErr(e error) {
	select {
	case c.errs <- e:
	default:
	}
}

// Called by consumers. The works as a demux or fanout if called multiple times. If a copy to
// each consumer is needed, that must be done externally
func (c *Convert) GetStream() <-chan [][]byte {
	return c.outgoing
}

func (c *Convert) ListenErr() <-chan error {
	return c.errs
}

func (c *Convert) ListenStats() <-chan string {
	return c.stats
}

func (c *Convert) Wait() {
	c.donewg.Wait()
}

func (c *Convert) Cancel() {
	c.canc()
}
