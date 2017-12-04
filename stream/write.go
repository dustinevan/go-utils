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

	outgoing chan [][]byte

	ctx     context.Context
	donewg  sync.WaitGroup
	monitor *Monitor
}

func NewWrite(w io.Writer, in <-chan [][]byte, opts ...WriteOption) (*Write, *Monitor) {
	ctx, canc := context.WithCancel(context.Background())

	wr := &Write{
		w:   w,
		ctx: ctx,
	}
	for _, opt := range opts {
		opt(wr)
	}

	wr.donewg.Add(1)
	monitor := NewMonitor(&wr.donewg, canc)

	wr.monitor = monitor

	go func() {
		defer wr.donewg.Done()
		wr.write(in)
	}()

	return wr, monitor

}

func (w *Write) write(in <-chan [][]byte) {
	bcount := 0
	mcount := 0
	start := time.Now()

	for chunk := range in {
		select {
		case <-w.ctx.Done():
			w.monitor.SubmitErr(fmt.Errorf("writestream: canceled"))
			continue
		default:
			for _, bytes := range chunk {
				bcount += len(bytes)
				mcount++
				_, err := w.w.Write(bytes)
				if err != nil {
					w.monitor.SubmitErr(err)
					w.monitor.SubmitStat(fmt.Sprintf("write failed, completed write of %v bytes, %v messages in %s",
						bcount, mcount, time.Since(start)))
				}
			}
		}
	}
	w.monitor.SubmitStat(fmt.Sprintf("successful write of %v bytes, %v messages in %s",
		bcount, mcount, time.Since(start)))
}
