package cache

import (
	"testing"
	"time"

	"github.com/mediaFORGE/supplyqc/vendor/github.com/stretchr/testify/assert"
)

func TestInsertMaxRecords(t *testing.T) {
	c := NewObjCache(2)

	assert.Nil(t, c.Insert("a", 1, time.Second),
		"unable to insert first obj into cache with size 2")
	assert.Nil(t, c.Insert("b", 2, time.Second),
		"unable to insert second obj into cache with size 2")
	err := c.Insert("c", 3, time.Second)
	assert.NotNil(t, err, "maxrecords failed to limit inserts")
	assert.Equal(t, CACHEFULL, err,
		"maxrecords didn't return CACHEFULL error")
}


func TestCheck(t *testing.T) {
	c := NewObjCache(2)

	c.Insert("a", 1, time.Second)
	c.Insert("b", 2, time.Second)
	c.Insert("c", 3, time.Second)

	assert.True(t, c.Check("a"), "check returned false for first inserted value")
	assert.True(t, c.Check("b"), "check returned false for second inserted value")
	assert.False(t, c.Check("c"), "check returned true for over the limit inserted value")
	assert.False(t, c.Check("d"), "check returned true for never inserted value")
}

func TestGet(t *testing.T) {
	c := NewObjCache(2)

	c.Insert("a", 1, time.Second)
	c.Insert("b", 2, time.Second)
	c.Insert("c", 3, time.Second)

	v, ok := c.Get("a")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 1, v.(int), "get return unexpected value %s", v)

	v, ok = c.Get("b")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 2, v.(int), "get return unexpected value %s", v)

	v, ok = c.Get("c")
	assert.False(t, ok, "get returned true for non-existent value")
	assert.Nil(t, v,  "get return unexpected value %s", v)
}

func TestUncache(t *testing.T) {
	c := NewObjCache(2)

	c.Insert("a", 1, time.Second)
	c.Insert("b", 2, time.Second)

	v, ok := c.Get("a")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 1, v.(int), "get return unexpected value %s", v)

	c.Uncache("a")

	v, ok = c.Get("a")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)

}

func TestUncacheMany(t *testing.T) {
	c := NewObjCache(5)

	c.Insert("a", 1, time.Second)
	c.Insert("b", 2, time.Second)
	c.Insert("c", 3, time.Second)
	c.Insert("d", 4, time.Second)
	c.Insert("e", 5, time.Second)

	v, ok := c.Get("a")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 1, v.(int), "get return unexpected value %s", v)
	v, ok = c.Get("b")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 2, v.(int), "get return unexpected value %s", v)
	v, ok = c.Get("c")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 3, v.(int), "get return unexpected value %s", v)
	v, ok = c.Get("d")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 4, v.(int), "get return unexpected value %s", v)
	v, ok = c.Get("e")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 5, v.(int), "get return unexpected value %s", v)

	c.UncacheMany([]string{"a","b","c"})

	v, ok = c.Get("a")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)
	v, ok = c.Get("b")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)
	v, ok = c.Get("c")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)
	v, ok = c.Get("d")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 4, v.(int), "get return unexpected value %s", v)
	v, ok = c.Get("e")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 5, v.(int), "get return unexpected value %s", v)

}

func TestDuplicateInserts(t *testing.T) {
	c := NewObjCache(5)

	c.Insert("a", 1, time.Second)
	c.Insert("b", 2, time.Second)
	c.Insert("c", 3, time.Second)
	c.Insert("d", 4, time.Second)
	c.Insert("e", 5, time.Second)

	v, ok := c.Get("a")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 1, v.(int), "get return unexpected value %s", v)

	c.Uncache("a")

	v, ok = c.Get("a")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)
}

func TestScanRate(t *testing.T) {

	c := NewObjCache(-1, ScanRate(time.Millisecond * 50))

	c.Insert("a", 1, time.Millisecond * 100)

	v, ok := c.Get("a")
	assert.True(t, ok, "get returned false for existing value")
	assert.Equal(t, 1, v.(int), "get return unexpected value %s", v)

	time.Sleep(time.Millisecond * 120)

	v, ok = c.Get("a")
	assert.False(t, ok, "get returned false for existing value")
	assert.Nil(t, v, "get return unexpected value %s", v)
}