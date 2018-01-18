package async

import (
	"context"
	"testing"
	"time"

	"github.com/dustinevan/go-utils/cache"
	"github.com/stretchr/testify/assert"
)

func TestUniQueue_Check(t *testing.T) {
	ctx, canc := context.WithCancel(context.Background())
	ch1 := NewUniQueue("test", 3, 3, time.Second, ctx)

	assert.Nil(t, ch1.Insert("a"), "first insert failed, three should work")
	assert.True(t, ch1.Check("a"), "dedup failed to store first inserted string")
	// wait for cachecleanup
	ch1.UnCache("a")
	assert.False(t, ch1.Check("d"), "dedup stored more inserts than allowed by maxdedup")
	canc()
	assert.False(t, ch1.Check("a"), "dedup failed to store first inserted string")
}

func TestUniQueue_Insert(t *testing.T) {
	ctx, canc := context.WithCancel(context.Background())
	ch1 := NewUniQueue("test1", 3, 5, time.Second, ctx)

	assert.Nil(t, ch1.Insert("a"), "first insert failed, three should work")
	assert.Equal(t, 1, ch1.records, "record count incorrect")
	assert.Nil(t, ch1.Insert("b"), "second insert failed, three should work")
	assert.Equal(t, 2, ch1.records, "record count incorrect")
	assert.Equal(t, cache.ALREADY_EXISTS, ch1.Insert("a"), "dedup failed")
	assert.Nil(t, ch1.Insert("c"), "third insert failed, three should work")
	assert.Equal(t, 3, ch1.records, "record count incorrect")
	assert.Equal(t, cache.ALREADY_EXISTS, ch1.Insert("b"), "dedup failed")
	assert.Equal(t, cache.CACHEFULL, ch1.Insert("d"), "cache grew larger than maxdedup")

	ch2 := NewUniQueue("test2", 12, 3, time.Second, ctx)

	assert.Nil(t, ch2.Insert("a"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("b"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("c"), "first insert failed, three should work")
	time.Sleep(time.Millisecond)
	assert.Nil(t, ch2.Insert("d"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("e"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("f"), "first insert failed, three should work")
	time.Sleep(time.Millisecond)
	assert.Nil(t, ch2.Insert("g"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("h"), "first insert failed, three should work")
	time.Sleep(time.Millisecond)
	assert.Nil(t, ch2.Insert("i"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("j"), "first insert failed, three should work")
	assert.Nil(t, ch2.Insert("k"), "first insert failed, three should work")
	assert.Equal(t, cache.ALREADY_EXISTS, ch2.Insert("a"), "dedup failed")

	// ch2.Insert may block if the code doesn't fulfil this test case, thus the go routine
	var err error
	go func() {
		err = ch2.Insert("l")
	}()
	time.Sleep(time.Millisecond * 100)
	if err == nil {
		assert.Fail(t, "inflight failed")
	} else {
		assert.Equal(t, QFULL, err, "inflight failed to return CHANFULL on insert")
	}
	assert.Equal(t, 11, ch2.records, "record count incorrect")
	assert.Equal(t, int32(3), ch2.inflight, "inflight count incorrect")
	assert.Equal(t, cache.ALREADY_EXISTS, ch1.Insert("a"), "dedup failed")

	// let the dedup cache clean out
	// unix time so 1 second is the lowest precision
	time.Sleep(time.Second * 2)
	assert.Equal(t, QFULL, ch2.Insert("a"), "dedup failed")
	assert.Equal(t, 0, ch2.records, "record count incorrect")

	c := ch2.GetChan()
	go func() { <-c }()
	// wait for to transfer from q to outgoing
	time.Sleep(time.Millisecond)
	assert.Nil(t, ch2.Insert("a"), "read didn't create any space")

	canc()
	assert.Error(t, ch2.Insert("z"), "error not returned on insert after cancel")
	time.Sleep(time.Millisecond)
	assert.Panics(t, func() { ch2.GetChan() }, "uniq failed to panic on GetChan() after close")
}

func TestUniQueue_UnCache(t *testing.T) {
	ctx, canc := context.WithCancel(context.Background())
	ch1 := NewUniQueue("test", 3, 3, time.Second, ctx)

	assert.Nil(t, ch1.Insert("a"), "first insert failed, three should work")
	assert.True(t, ch1.Check("a"), "dedup failed to store first inserted string")
	// wait for cachecleanup
	ch1.UnCache("a")
	assert.False(t, ch1.Check("d"), "dedup stored more inserts than allowed by maxdedup")
	canc()
}

func TestUniQueue_Close(t *testing.T) {

}
