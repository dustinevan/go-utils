package stream

import (
	"context"
	"fmt"
	"os"
)

type CatOption func(*Cat) *Cat

type Cat struct {
	fn                   ConvertFn
	absfilepath          []string
	ctx                  context.Context
	concurrentConverters int
}

func CatFiles(fn ConvertFn, absfilenames []string, ctx context.Context, opts ...CatOption) error {
	if len(absfilenames) == 0 {
		return fmt.Errorf("no files passed to CatFiles")
	}

	rs := &Cat{
		fn:                   fn,
		ctx:                  ctx,
		concurrentConverters: 1,
	}
	for _, opt := range opts {
		opt(rs)
	}

	rmons := make([]*Monitor, len(absfilenames))
	rstreams := make([]InChan, len(absfilenames))
	if len(absfilenames) == 1 && absfilenames[0] == "stdin" {
		read, mon := NewRead(NewFileReader(os.Stdin), ChunkSize(1024))
		rstreams[0] = read.GetStream()
		rmons[0] = mon
	} else {
		for i, name := range absfilenames {
			option, err := FileToRead(name)
			if err != nil {
				return err
			}
			read, mon := NewRead(NewFileReader(nil, option), ChunkSize(1024))
			rstreams[i] = read.GetStream()
			rmons[i] = mon
		}
	}

	// TODO: don't join if there's only one
	readmon := JoinMonitors(rmons...)

	convert, monc := NewConvert(rs.fn, rstreams)
	_, monw := NewWrite(os.Stdout, []InChan{convert.GetStream()})

	readmon.CancelOnErr(func() {
		readmon.CancelRoutine()
		monc.CancelRoutine()
		monw.CancelRoutine()
	})
	monw.CancelOnErr(func() {
		readmon.CancelRoutine()
		monc.CancelRoutine()
		monw.CancelRoutine()
	})
	monc.Log()

	monw.GetSuccess()
	return nil
}
