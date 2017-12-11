package stream

import (
	"context"
	"fmt"
	"os"
	"sync"
	"github.com/dustinevan/go-utils/async"
)

type CatOption func(*cat) error

func CatContext(ctx context.Context) CatOption {
	return func(c *cat) error {
		c.ctx = ctx
		return nil
	}
}


func ConcurrentStreams(i int) CatOption {
	return func(c *cat) error {
		if i < 1 {
			panic("cannot have maxconcurrency < 1")
		}
		c.maxconcurrency = i
		return nil
	}
}

type cat struct {
	fn                   ConvertFn
	absfilenames          []string
	ctx                  context.Context
	maxconcurrency int
}

func CatFiles(fn ConvertFn, absfilenames []string, opts ...CatOption ) error {
	c := &cat{
		fn: fn,
		absfilenames: absfilenames,
		ctx: context.Background(),
		maxconcurrency: 4,
	}
	for _, opt := range opts {
		opt(c)
	}

	if len(absfilenames) == 0 {
		return fmt.Errorf("no files passed to CatFiles")
	}

	if len(absfilenames) == 1 && absfilenames[0] == "stdin" {
		rdr, err := NewFileReader("stdin")
		if err != nil {
			return err
		}
		r := NewRead(rdr, ChunkSize(1))
		c := NewConvert(fn, r.GetStream())
		w := NewWrite(os.Stdout, c.GetStream())

		monit := NewMonitor(c.ctx)
		monit.Register(r, CancelOnErr)
		monit.Register(c, LogAll)
		monit.Register(w, CancelOnErr)
		w.Wait()
		return nil
	}

	sem := async.NewSemaphore(c.maxconcurrency, c.ctx)
	var wg sync.WaitGroup
	for _, filen := range absfilenames {
		err := sem.Acquire()
		if err != nil {
			continue
		}
		wg.Add(1)
		rdr, err := NewFileReader(filen)
		if err != nil {
			return err
		}
		r := NewRead(rdr, ChunkSize(1024))
		c := NewConvert(fn, r.GetStream())
		w := NewWrite(os.Stdout, c.GetStream())

		go func() {
			defer wg.Done()
			defer sem.Release()
			w.Wait()
		}()
	}

	wg.Wait()
	return nil
}
