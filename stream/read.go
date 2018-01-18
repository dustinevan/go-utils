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
	r RecordReader

	chunksize int
	outgoing  chan [][]byte

	ctx     context.Context
	donewg  sync.WaitGroup
	monitor *Monitor
}

func NewRead(r RecordReader, opts ...ReadOption) (*Read, *Monitor) {
	ctx, canc := context.WithCancel(context.Background())

	read := &Read{
		r:         r,
		chunksize: 100,
		outgoing:  make(chan [][]byte, 16),
		ctx:       ctx,
	}
	for _, opt := range opts {
		opt(read)
	}

	read.donewg.Add(1)
	monitor := NewMonitor(&read.donewg, canc)

	read.monitor = monitor

	go func() {
		defer close(read.outgoing)
		defer read.donewg.Done()
		read.read()
	}()

	return read, monitor
}

func (r *Read) read() {
	bcount := 0
	mcount := 0
	start := time.Now()
	for {
		select {
		case <-r.ctx.Done():
			r.monitor.SubmitErr(fmt.Errorf("readstream: canceled"))
			return
		default:
			chunk := make([][]byte, r.chunksize)
			for i := 0; i < r.chunksize; i++ {
				bytes, err := r.r.Read()
				if err != nil && err != io.EOF {
					r.monitor.SubmitErr(err)
					r.monitor.SubmitStat(fmt.Sprintf("unsuccessful read of: %s read %v bytes %v message in %s",
						r.r.Name(), bcount, mcount, time.Since(start)))
					return
				}
				if err == io.EOF {
					r.outgoing <- chunk[:i]
					r.monitor.SetSuccess(true)
					r.monitor.SubmitStat(fmt.Sprintf("successful read of: %s read %v bytes %v message in %s",
						r.r.Name(), bcount, mcount, time.Since(start)))
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

func (r *Read) GetStream() <-chan [][]byte {
	return r.outgoing
}
