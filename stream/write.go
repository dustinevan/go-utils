package stream

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
)

type StreamWriter struct {
	w io.Writer

	ctx  context.Context
	canc context.CancelFunc

	parentwg *sync.WaitGroup
	// this wg is for the multiple incoming channels (see SetStream) so we know when
	// to close incoming and complete

	muxwg     sync.WaitGroup
	incoming  chan [][]byte
	isWaiting int32
	mu        sync.Mutex

	completed chan struct{}
}

func NewStreamWriter(w io.Writer, wg *sync.WaitGroup, ctx context.Context, can context.CancelFunc) *StreamWriter {
	s := &StreamWriter{
		w: w,

		ctx:      ctx,
		canc:     can,
		parentwg: wg,

		muxwg:    sync.WaitGroup{},
		incoming: make(chan [][]byte, 16),

		completed: make(chan struct{}, 1),
	}

	go func() {
		s.write()
	}()
	return s
}

// SetStream works as a Mux into the incoming channel.
func (s *StreamWriter) SetStream(ch <-chan [][]byte) {
	s.mu.Lock()
	s.mu.Unlock()
	// write data from ch to incoming
	s.muxwg.Add(1)
	go func() {
		for b := range ch {
			s.incoming <- b
		}
		s.muxwg.Done()
	}()

	s.waitAndClose()
}

func (s *StreamWriter) waitAndClose() {
	// if the goroutine already exists, return
	if atomic.LoadInt32(&s.isWaiting) > 0 {
		return
	}
	atomic.AddInt32(&s.isWaiting, 1)

	// start goroutine that will close incoming when all the channels writing to it close
	go func() {
		s.muxwg.Wait()
		close(s.incoming)
	}()
}

func (s *StreamWriter) write() {
	defer s.parentwg.Done()
	for chunk := range s.incoming {
		select {
		case <-s.ctx.Done():
			continue
		default:
			for _, bytes := range chunk {
				_, err := s.w.Write(bytes)
				if err != nil {
					log.Println("streamwriter: encountered write error, canceling stream.")
					s.canc()
				}
			}
		}
	}
	close(s.completed)
}
