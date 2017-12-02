package stream

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

type ConvertFn func(b []byte) ([]byte, error)

type ConvertOption func(c *Convert) *Convert

func CancelOnConvertErr() ConvertOption {
	return func(c *Convert) *Convert {
		c.handler = func(err error, rec []byte) {
			log.Printf("converter: encountered error: %s on record: %s, canceling stream.", err, string(rec))
			c.canc()
		}
		return c
	}
}

type Convert struct {
	convertfn ConvertFn

	ctx  context.Context
	canc context.CancelFunc

	// handle n possible calls to SetStream
	muxwg     sync.WaitGroup
	incoming  chan [][]byte
	isWaiting int32

	// returned when GetStream is called
	mu       sync.Mutex
	outgoing chan [][]byte

	// how should convert errors be handled?
	handler func(err error, rec []byte)
}

func NewConvert(convertfn ConvertFn, ctx context.Context, can context.CancelFunc, opts ...ConvertOption) *Convert {
	c := &Convert{
		convertfn: convertfn,

		ctx:  ctx,
		canc: can,

		muxwg:    sync.WaitGroup{},
		incoming: make(chan [][]byte, 4),
		outgoing: make(chan [][]byte, 4),
		handler: func(err error, rec []byte) {
			log.Printf("converter: encountered error: %s on record: %s, canceling stream.", err, string(rec))
		},
	}

	go func() {
		c.convert()
	}()
	return c
}

// SetStream works as a Mux into the incoming channel.
func (c *Convert) SetStream(ch <-chan [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// write data from ch to incoming
	c.muxwg.Add(1)
	go func() {
		for b := range ch {
			c.incoming <- b
		}
		c.muxwg.Done()
	}()

	c.waitAndClose()
}

func (c *Convert) waitAndClose() {
	// if the goroutine already exists, return
	if atomic.LoadInt32(&c.isWaiting) > 0 {
		return
	}
	atomic.AddInt32(&c.isWaiting, 1)

	// start goroutine that will close incoming when all the channels writing to it close
	go func() {
		c.muxwg.Wait()
		close(c.incoming)
	}()
}

// Called by consumers. The works as a demux or fanout if called multiple times. If a copy to
// each consumer is needed, that must be done externally
func (c *Convert) GetStream() <-chan [][]byte {
	return c.outgoing
}

func (c *Convert) convert() {
	defer close(c.outgoing)
	for chunk := range c.incoming {
		select {
		case <-c.ctx.Done():
			continue
		default:
			bufs := make([][]byte, len(chunk))
			i := 0
			for _, bytes := range chunk {
				buf, err := c.convertfn(bytes)
				if err != nil {
					c.handler(err, bytes)
					continue
				}
				bufs[i] = buf
				i++
			}
			c.outgoing <- bufs[:i-1]
		}
	}
}
