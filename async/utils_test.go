package async

import (
	"testing"
	"time"
	"context"
	"github.com/stretchr/testify/assert"
)

func TestTickerLog(t *testing.T) {
	ctx, canc := context.WithCancel(context.Background())
	i := 0
	TickerLog(time.Millisecond, ctx, func() {i++})
	time.Sleep(time.Microsecond * 10500)
	canc()
	assert.Equal(t, 10, i, "failed to run the logfn the correct number of times")
}

func TestDrains(t *testing.T) {
	sg := make(chan string, 2)
	st := make(chan struct{}, 2)

	sg<-"a"
	sg<-"b"
	st<-struct{}{}
	st<-struct{}{}
	assert.Panics(t, func() { drainString(sg) }, "missing channel close did not panic")
	assert.Panics(t, func() { drainStruct(st) }, "missing channel close did not panic")

	sg<-"a"
	sg<-"b"
	st<-struct{}{}
	st<-struct{}{}

	go func() {
		sg<-"c"
		close(sg)
	}()

	go func() {
		st<-struct{}{}
		close(st)
	}()

	assert.NotPanics(t, func() { drainString(sg) }, "missing channel close did not panic")
	assert.NotPanics(t, func() { drainStruct(st) }, "missing channel close did not panic")
}