package stream

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"sync"
)

type RecordReader interface {
	Read() ([]byte, error)
}

type FileReaderOption func(f *FileReader) *FileReader

func Delim(d byte) FileReaderOption {
	return func(r *FileReader) *FileReader {
		r.delim = d
		return r
	}
}

//TODO: Gzip, Lzop Options

type FileReader struct {
	buf        *bufio.Reader
	decompress func(r io.Reader) (io.Reader, error)
	delim      byte
	wg         sync.WaitGroup
	ready      bool
	empty      bool
}

func NewFileReader(r io.Reader, opts ...FileReaderOption) *FileReader {
	f := &FileReader{
		delim: '\n',
	}
	for _, opt := range opts {
		opt(f)
	}

	if f.decompress == nil {
		f.buf = bufio.NewReader(r)
		return f
	}

	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		decmp, err := f.decompress(r)
		if err != nil {
			f.empty = true
			f.ready = true
			log.Printf("decompression failed %s")
			return
		}
		f.buf = bufio.NewReader(decmp)
		f.ready = true
	}()

	return f
}

func (f *FileReader) Read() ([]byte, error) {
	if !f.ready {
		f.wg.Wait()
	}
	if f.empty {
		return nil, fmt.Errorf("decompression failed")
	}
	return f.buf.ReadBytes(f.delim)
}
