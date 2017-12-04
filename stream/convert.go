package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ConvertFn func(b []byte) ([]byte, error)

type ConvertOption func(c *Convert) *Convert

//func CancelOnConvertErr(canc context.CancelFunc) ConvertOption {
//	return func(c *Convert) *Convert {
//		c.handler = func(err error, rec []byte) {
//			log.Printf("converter: encountered error: %s on record: %s, canceling stream.", err, string(rec))
//			canc()
//		}
//		return c
//	}
//}

type Convert struct {
	fn ConvertFn

	outgoing chan [][]byte

	ctx     context.Context
	donewg  sync.WaitGroup
	monitor *Monitor
}

func NewConvert(fn ConvertFn, in <-chan [][]byte, opts ...ConvertOption) (*Convert, *Monitor) {
	ctx, canc := context.WithCancel(context.Background())

	c := &Convert{
		fn: fn,

		outgoing: make(chan [][]byte, 16),
		ctx:      ctx,
	}
	for _, opt := range opts {
		opt(c)
	}

	c.donewg.Add(1)
	monitor := NewMonitor(&c.donewg, canc)

	c.monitor = monitor

	go func() {
		defer close(c.outgoing)
		defer c.donewg.Done()
		c.convert(in)
	}()
	return c, monitor
}

func (c *Convert) convert(in <-chan [][]byte) {
	success := 0
	failed := 0
	start := time.Now()
	defer c.monitor.SubmitStat(fmt.Sprintf("successful convert of %v messages, %v messages failed in %s",
		success, failed, time.Since(start)))

	for chunk := range in {
		select {
		case <-c.ctx.Done():
			c.monitor.SubmitErr(fmt.Errorf("convertstream: canceled"))
			continue
		default:
			bufs := make([][]byte, len(chunk))
			i := 0
			for _, bytes := range chunk {
				buf, err := c.fn(bytes)
				if err != nil {
					c.monitor.SubmitErr(fmt.Errorf("converter: encountered error: %s on record: %s, skipping", err, string(bytes)))
					failed++
					continue
				}
				success++
				bufs[i] = buf
				i++
			}
			c.outgoing <- bufs[:i-1]
		}
	}
	if failed == 0 {
		c.monitor.SetSuccess(true)
	}
}

// Called by consumers. The works as a demux or fanout if called multiple times. If a copy to
// each consumer is needed, that must be done externally
func (c *Convert) GetStream() <-chan [][]byte {
	return c.outgoing
}