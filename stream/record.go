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

type FileReaderOption func(f *FileReader) *FileReader

func Delim(d byte) FileReaderOption {
	return func(r *FileReader) *FileReader {
		r.delim = d
		return r
	}
}

func FileToRead(absfilename string) (FileReaderOption, error) {
	file, err := os.Open(absfilename)
	if err != nil {
		return nil, err
	}
	return func(f *FileReader) *FileReader {
		f.file = file
		f.postread = append(f.postread, func() {
			err := file.Close()
			if err != nil {
				log.Println(err)
			}

		})
		return f
	}, nil
}

//TODO: Gzip, Lzop Options

type FileReader struct {
	buf   *bufio.Reader
	file  *os.File
	delim byte

	decompress func(string) (*os.File, error)
	filename   string

	postread []func()

	wg    sync.WaitGroup
	ready bool
	empty bool
}

func NewFileReader(file *os.File, opts ...FileReaderOption) *FileReader {
	f := &FileReader{
		delim: '\n',
		file:  file,
	}
	for _, opt := range opts {
		opt(f)
	}

	if f.decompress == nil {
		f.buf = bufio.NewReader(file)
		return f
	}

	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		df, err := f.decompress(f.filename)
		if err != nil {
			f.empty = true
			f.ready = true
			log.Printf("decompression failed %s")
			return
		}
		f.buf = bufio.NewReader(df)
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

func (f *FileReader) Close() {
	for _, fn := range f.postread {
		fn()
	}
}

func (f *FileReader) Name() string {
	return f.file.Name()
}
