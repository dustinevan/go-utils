package async

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSideEffectWorker(t *testing.T) {
	out := "a"
	workfn := func(s string, ctx context.Context) {
		out = s
	}

	ctx, canc := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	ch := make(chan string, 2)
	ch2 := make(chan string, 2)
	ch3 := make(chan string, 2)

	w := NewSideEffectWorker(ctx, &wg, workfn)
	w.SetWorkChan(ch)

	ch <- "test"
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test", out, "worker failed to run work function")

	w.SetWorkChan(ch2)

	ch2 <- "test1"
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test1", out, "worker failed to run work function")

	ch <- "test2"
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test2", out, "worker failed to run work function")

	close(ch)

	ch2 <- "test3"
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test3", out, "worker failed to run work function")

	close(ch2)

	blocked := true
	go func() {
		wg.Wait()
		blocked = false
	}()

	time.Sleep(time.Millisecond)
	assert.Equal(t, true, blocked, "wait must block until close is called")
	canc()
	time.Sleep(time.Millisecond)
	assert.Equal(t, false, blocked, "wait group blocked, internal channel wasn't closed properly")

	assert.Panics(t, func() { w.SetWorkChan(ch3) }, "calls to SetWorkChan on closed workers should panic")

	ctx1, canc := context.WithCancel(context.Background())
	w1 := NewSideEffectWorker(ctx1, &wg, workfn)
	canc()
	time.Sleep(time.Millisecond)
	assert.Panics(t, func() { w1.SetWorkChan(ch3) }, "calls to SetWorkChan on closed workers should panic")
}
