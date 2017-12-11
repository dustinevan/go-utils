package stream

import (
	"os"
	"bufio"
	"sync"
	"fmt"
	"log"
)

type RecordReader interface {
	Read() ([]byte, error)
	Close()
	Name() string
}

type FileReaderOption func(f *FileReader) error

func Delim(d byte) FileReaderOption {
	return func(r *FileReader) error {
		r.delim = d
		return nil
	}
}

//TODO: Gzip, Lzop Options

type FileReader struct {
	buf   *bufio.Reader
	file  *os.File
	delim byte

	decompress func(file *os.File) (*os.File, error)
	filename   string

	postread []func()

	decompresswg sync.WaitGroup
	ready        bool
	empty        bool
}

func NewFileReader(filename string, opts ...FileReaderOption) (*FileReader, error) {
	r := &FileReader{
		delim:    '\n',
		postread: make([]func(), 0),
	}
	if filename == "stdin" {
		r.file = os.Stdin
	} else {
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		r.file = f
		r.postread = append(r.postread, func() {
			err := f.Close()
			if err != nil {
				log.Println(err)
			}
		})
	}

	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	// if decompress is set, run decompression, and replace file, replace buf, and add a close func to postread
	if r.decompress != nil {
		r.decompresswg.Add(1)
		go func() {
			defer r.decompresswg.Done()
			df, err := r.decompress(r.file)
			if err != nil {
				r.empty = true
				r.ready = true
				log.Printf("decompression failed %s", err)
				return
			}
			r.file = df
			r.postread = append(r.postread, func() {
				err := df.Close()
				if err != nil {
					log.Println(err)
				}
			})
			r.ready = true
			r.buf = bufio.NewReader(r.file)
		}()
	} else {
		r.buf = bufio.NewReader(r.file)
		r.ready = true
	}


	return r, nil
}

func (r *FileReader) Read() ([]byte, error) {
	if !r.ready {
		r.decompresswg.Wait()
	}
	if r.empty {
		return nil, fmt.Errorf("decompression failed")
	}
	bytes, err := r.buf.ReadBytes(r.delim)
	return bytes, err

}

func (r *FileReader) Close() {
	for _, fn := range r.postread {
		fn()
	}
}

func (r *FileReader) Name() string {
	return r.file.Name()
}
