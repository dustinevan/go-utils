package async

import (
	"context"
	"sync"
)

// A Stream is a struct with a non-blocking Insert and a GetChan that returns a read-only channel
// When the insert would have blocked, or the stream is closed an error is returned.
// Streams should likely be closed by context.
// String was chosen because it can be a single arg, a list of delimited args, or json
type Stream interface {
	Insert(string) error
	GetChan() <-chan string
}

// A Pool is a struct that performs a preset operation given the submitted string
// Submit is non-blocking. It returns an error if it would have blocked, if the pool has been closed
// or if a submission error occurs
type WorkerPool interface {
	Submit(string) error
	Close()
}

// A work function that has sideeffects rather than return values.
type SideEffectFn func(string, context.Context)

// This worker pool doesn't block on submit or repeat operations over the time period specified by the passed uniqueue
type DedupWorkerPool struct {
	uniq  *UniQueue
	wg    sync.WaitGroup
	canc  context.CancelFunc
	qcanc context.CancelFunc
}

func NewDedupWorkerPool(workers int, uniq *UniQueue, se SideEffectFn, parentCtx context.Context, qcanc context.CancelFunc) *DedupWorkerPool {

	ctx, canc := context.WithCancel(parentCtx)
	wp := &DedupWorkerPool{
		uniq:  uniq,
		canc:  canc,
		qcanc: qcanc,
	}

	for i := 0; i < workers; i++ {
		// The parentCtx is passed down to the work function. This is used to cancel long running operations
		// The workers themselves only finish when their work channels are closed.
		w := NewSideEffectWorker(ctx, &wp.wg, se)
		w.SetWorkChan(wp.uniq.GetChan())
	}
	return wp
}

func (w *DedupWorkerPool) Close() {
	// stop the workq nicely
	w.qcanc()
	w.canc()
	w.wg.Wait()
}

func (w *DedupWorkerPool) Submit(work string) error {
	return w.uniq.Insert(work)
}
