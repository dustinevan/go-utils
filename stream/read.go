package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type ReadOption func(stream *Read) *Read

func ChunkSize(n int) ReadOption {
	return func(stream *Read) *Read {
		stream.chunksize = n
		return stream
	}
}

type Read struct {
	rdr RecordReader

	chunksize int
	outgoing  chan [][]byte

	ctx    context.Context
	canc   context.CancelFunc
	donewg sync.WaitGroup

	errs  chan error
	stats chan string
}

func NewRead(rdr RecordReader, opts ...ReadOption) *Read {
	ctx, canc := context.WithCancel(context.Background())

	r := &Read{
		rdr: rdr,

		chunksize: 100,
		outgoing:  make(chan [][]byte, 16),

		ctx:  ctx,
		canc: canc,

		errs:  make(chan error, 10000),
		stats: make(chan string, 8),
	}
	for _, opt := range opts {
		opt(r)
	}

	r.donewg.Add(1)
	go func() {
		defer close(r.outgoing)
		defer r.donewg.Done()
		r.read()
	}()

	return r
}

func (r *Read) read() {
	bcount := 0
	mcount := 0
	start := time.Now()
	for {
		select {
		case <-r.ctx.Done():
			r.submitErr(fmt.Errorf("readstream: canceled"))
			return
		default:
			chunk := make([][]byte, r.chunksize)
			for i := 0; i < r.chunksize; i++ {
				bytes, err := r.rdr.Read()
				if err != nil && err != io.EOF {
					r.submitErr(fmt.Errorf("read of %s failed, %s", r.rdr.Name(), err))
					r.submitStat(fmt.Sprintf("unsuccessful read of: %s read %v bytes %v message in %s",
						r.rdr.Name(), bcount, mcount, time.Since(start)))
					return
				}
				if err == io.EOF {
					r.outgoing <- chunk[:i]
					r.submitStat(fmt.Sprintf("successful read of: %s read %v bytes %v message in %s",
						r.rdr.Name(), bcount, mcount, time.Since(start)))

					return
				}
				bcount += len(bytes)
				mcount++
				chunk[i] = bytes
			}
			r.outgoing <- chunk
		}
	}
}

func (r *Read) submitStat(s string) {
	select {
	case r.stats <- s:
	default:
	}
}

func (r *Read) submitErr(e error) {
	select {
	case r.errs <- e:
	default:
	}
}

func (r *Read) GetStream() <-chan [][]byte {
	return r.outgoing
}

func (r *Read) ListenErr() <-chan error {
	return r.errs
}

func (r *Read) ListenStats() <-chan string {
	return r.stats
}

func (r *Read) Wait() {
	r.donewg.Wait()
}

func (r *Read) Cancel() {
	r.canc()
}
