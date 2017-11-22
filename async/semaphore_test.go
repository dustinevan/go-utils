package async

import (
	"testing"
	"context"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestSemaphore(t *testing.T) {
	ctx, canc := context.WithCancel(context.Background())
	s1 := NewSemaphore(1, ctx)

	assert.Nil(t, s1.Acquire(), "error when acquiring lock")
	done := false
	go func() {
		s1.Acquire()
		done = true
	}()
	time.Sleep(time.Millisecond)
	assert.False(t, done, "acquired unavailable lock")
	s1.Release()
	time.Sleep(time.Millisecond)
	assert.True(t, done, "didn't properly release lock")

	canc()
	assert.Error(t, s1.Acquire(), "parent context cancel failed to close semaphore")
	//s2 := NewSemaphore(2, context.Background())
}