package stream

import (
	"context"
	"io"
	"log"

	"github.com/dustinevan/protobuf/records"
)

type ReaderOption func(stream *Read) *Read

func ChunkSize(n int) ReaderOption {
	return func(stream *Read) *Read {
		stream.chunksize = n
		return stream
	}
}

type Read struct {
	r records.Reader

	ctx  context.Context
	canc context.CancelFunc
	chunksize int

	outgoing chan [][]byte

}

func NewRead(r records.Reader, ctx context.Context, canc context.CancelFunc, opts ...ReaderOption) *Read {
	return &Read{
		r: r,

		ctx:  ctx,
		canc: canc,
		chunksize: 100,
		outgoing: make(chan [][]byte, 16),
	}
}

func (s *Read) Start() {
	go func() {
		s.read()
	}()
}

func (s *Read) read() {
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
func (s *Read) GetStream() <-chan [][]byte {
	return s.outgoing
}

// for the future
//func (w *Read) Monitor() <-chan monitor.Message {
//
//}
