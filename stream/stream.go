package stream

import (
	"context"
	"os"
)

type RecordStreamOption func(*RecordStream) *RecordStream

type RecordStream struct {
	fn                   ConvertFn
	absfilepath          string
	ctx                  context.Context
	concurrentConverters int
}

func StreamRecords(fn ConvertFn, ctx context.Context, opts ...RecordStreamOption) error {
	rs := &RecordStream{
		fn:                   fn,
		ctx:                  ctx,
		concurrentConverters: 1,
	}
	for _, opt := range opts {
		opt(rs)
	}
	var rr *FileReader
	if rs.absfilepath == "" {
		rr = NewFileReader(os.Stdin)
	} else {
		option, err := FileToRead(rs.absfilepath)
		if err != nil {
			return err
		}
		rr = NewFileReader(nil, option)
	}

	read, _ := NewRead(rr)
	convert, _ := NewConvert(rs.fn, read.GetStream())
	_, wrmonit := NewWrite(os.Stdout, convert.GetStream())

	wrmonit.GetSuccess()
	return nil
}
