package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type WriteOption func(c *Write) *Write

// TODO: make record writer
type Write struct {
	w io.Writer

	ctx    context.Context
	canc   context.CancelFunc
	donewg sync.WaitGroup

	errs  chan error
	stats chan string
}

func NewWrite(w io.Writer, in <-chan [][]byte, opts ...WriteOption) *Write {
	ctx, canc := context.WithCancel(context.Background())

	wr := &Write{
		w: w,

		ctx:  ctx,
		canc: canc,

		errs:  make(chan error, 10000),
		stats: make(chan string, 8),
	}
	for _, opt := range opts {
		opt(wr)
	}

	wr.donewg.Add(1)
	go func() {
		defer wr.donewg.Done()
		wr.write(in)
	}()

	return wr
}

func (w *Write) write(in <-chan [][]byte) {
	bcount := 0
	mcount := 0
	start := time.Now()

	for chunk := range in {

		select {
		case <-w.ctx.Done():
			w.submitErr(fmt.Errorf("writestream: canceled"))
			continue
		default:
			for _, bytes := range chunk {
				bcount += len(bytes)
				mcount++
				_, err := w.w.Write(append(bytes, '\n'))
				if err != nil {
					w.submitStat(fmt.Sprintf("write failed, completed write of %v bytes, %v messages in %s",
						bcount, mcount, time.Since(start)))
					w.submitErr(err)
				}
			}
		}
	}
	w.submitStat(fmt.Sprintf("successful write of %v bytes, %v messages in %s",
		bcount, mcount, time.Since(start)))
}

func (w *Write) submitStat(s string) {
	select {
	case w.stats <- s:
	default:
	}
}

func (w *Write) submitErr(e error) {
	select {
	case w.errs <- e:
	default:
	}
}

func (w *Write) ListenErr() <-chan error {
	return w.errs
}

func (w *Write) ListenStats() <-chan string {
	return w.stats
}

func (w *Write) Wait() {
	w.donewg.Wait()
}

func (w *Write) Cancel() {
	w.canc()
}
