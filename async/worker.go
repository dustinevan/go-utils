package async

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/simpleuuid"
)

type SideEffectWorker struct {
	fn SideEffectFn

	parentwg *sync.WaitGroup
	ctx      context.Context
	id       string

	muxwg     sync.WaitGroup
	incoming  chan string
	isWaiting int32
	closed    int32
}

func NewSideEffectWorker(ctx context.Context, parentwg *sync.WaitGroup, fn SideEffectFn) *SideEffectWorker {
	uuid, _ := simpleuuid.NewTime(time.Now())
	s := &SideEffectWorker{
		fn:       fn,
		parentwg: parentwg,
		ctx:      ctx,
		incoming: make(chan string, 1),
		id:       uuid.String(),
	}

	go func() {
		s.parentwg.Add(1)
		defer s.parentwg.Done()
		s.work()
	}()

	// shutdown goroutine
	go func() {
		<-s.ctx.Done()
		atomic.AddInt32(&s.closed, 1)
		s.muxwg.Wait()
		close(s.incoming)
	}()

	return s
}

func (w *SideEffectWorker) SetWorkChan(ch <-chan string) {
	if atomic.LoadInt32(&w.closed) == 1 {
		panic("Worker is closed")
	}

	// write data from ch to incoming
	w.muxwg.Add(1)
	go func() {
		for b := range ch {
			w.incoming <- b
		}
		w.muxwg.Done()
	}()
}

func (w *SideEffectWorker) work() {
	for d := range w.incoming {
		w.fn(d, w.ctx)
	}
}
