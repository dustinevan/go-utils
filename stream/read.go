package stream

import (
	"context"
	"io"
	"log"

	"github.com/dustinevan/protobuf/records"
)

type ReaderOption func(stream *ReadStream) *ReadStream

func ChunkSize(n int) ReaderOption {
	return func(stream *ReadStream) *ReadStream {
		stream.chunksize = n
		return stream
	}
}

type ReadStream struct {
	r records.Reader

	ctx  context.Context
	canc context.CancelFunc
	chunksize int

	outgoing chan [][]byte

}

func NewReadStream(r records.Reader, ctx context.Context, canc context.CancelFunc, opts ...ReaderOption) *ReadStream {
	return &ReadStream{
		r: r,

		ctx:  ctx,
		canc: canc,
		chunksize: 100,
		outgoing: make(chan [][]byte, 16),
	}
}

func (s *ReadStream) Start() {
	go func() {
		s.read()
	}()
}

func (s *ReadStream) read() {
	defer close(s.outgoing)
	for {
		select {
		case <-s.ctx.Done():
			log.Println("readstream: canceled")
			return
		default:
			chunk := make([][]byte, s.chunksize)
			for i := 0; i < s.chunksize; i++{
				bytes, err := s.r.Read()
				if err != nil && err != io.EOF {
					log.Println("readstream: encountered read error, canceling stream.", err)
					s.canc()
					return
				}
				if err == io.EOF {
					s.outgoing <- chunk[:i]
					return
				}
				chunk[i] = bytes
			}
			s.outgoing <- chunk
		}
	}
}

// Called by consumers. The works as a demux or fanout if called multiple times. If a copy to
// each consumer is needed, that must be done externally
func (s *ReadStream) GetStream() <-chan [][]byte {
	return s.outgoing
}

// for the future
//func (w *ReadStream) Monitor() <-chan monitor.Message {
//
//}
