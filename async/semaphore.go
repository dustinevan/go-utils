package async

import (
	"context"

	"github.com/pkg/errors"
)

type Semaphore struct {
	buf    chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

func NewSemaphore(max int, parentCtx context.Context) *Semaphore {
	ctx, canc := context.WithCancel(parentCtx)

	s := &Semaphore{
		buf:    make(chan struct{}, max),
		ctx:    ctx,
		cancel: canc,
	}

	go func() {
		<-ctx.Done()
		close(s.buf)
	}()

	return s
}

var CLOSED = errors.New("the semaphore has been closed")

func (s *Semaphore) Acquire() error {
	select {
	case <-s.ctx.Done():
		return CLOSED
	case s.buf <- struct{}{}:
		return nil
	}
}

func (s *Semaphore) Release() {
	<-s.buf
}

func (s *Semaphore) Close() {
	s.cancel()
	drainStruct(s.buf)
}
