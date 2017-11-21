package async

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDedupWorkerPool_Submit(t *testing.T) {
	out := "a"
	workfn := func(s string, ctx context.Context) {
		out = s
	}
	ctx, canc := context.WithCancel(context.Background())
	ch1 := NewUniQueue("test", 3, 3, time.Second, ctx)

	p := NewDedupWorkerPool(10, ch1, workfn, ctx, canc)

	p.Submit("test")
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test", out, "pool failed to do the work")

	p.Submit("test1")
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test1", out, "pool failed to do the work")

	p.Close()
	err := p.Submit("test2")
	time.Sleep(time.Millisecond)
	assert.Equal(t, "test1", out, "submitted work was still done after close")
	assert.Equal(t, UNIQUEUE_CLOSED, err, "closing pool did not result in a closed work stream")
}
