package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeoutCache_InsertMaxRecords(t *testing.T) {
	c := NewTimeoutCache(2)

	assert.Nil(t, c.Insert("a", time.Second),
		"unable to insert first obj into cache with size 2")
	assert.Nil(t, c.Insert("b", time.Second),
		"unable to insert second obj into cache with size 2")
	err := c.Insert("c", time.Second)
	assert.NotNil(t, err, "maxrecords failed to limit inserts")
	assert.Equal(t, CACHEFULL, err,
		"maxrecords didn't return CACHEFULL error")
}

func TestTimeoutCache_Check(t *testing.T) {
	c := NewTimeoutCache(2)

	c.Insert("a", time.Second)
	c.Insert("b", time.Second)
	c.Insert("c", time.Second)

	assert.True(t, c.Check("a"), "check returned false for first inserted value")
	assert.True(t, c.Check("b"), "check returned false for second inserted value")
	assert.False(t, c.Check("c"), "check returned true for over the limit inserted value")
	assert.False(t, c.Check("d"), "check returned true for never inserted value")
}

func TestTimeoutCache_Uncache(t *testing.T) {
	c := NewTimeoutCache(2)

	c.Insert("a", time.Second)
	c.Insert("b", time.Second)

	assert.True(t, c.Check("a"), "check returned false for existing value")
	assert.True(t, c.Check("b"), "check returned false for existing value")

	c.Uncache("a")

	assert.False(t, c.Check("a"), "check returned true for uncached value")
	assert.True(t, c.Check("b"), "check returned false for existing value")
}

func TestTimeoutCache_UncacheMany(t *testing.T) {
	c := NewTimeoutCache(5)

	c.Insert("a", time.Second)
	c.Insert("b", time.Second)
	c.Insert("c", time.Second)
	c.Insert("d", time.Second)
	c.Insert("e", time.Second)

	assert.True(t, c.Check("a"), "check returned false for existing value")
	assert.True(t, c.Check("b"), "check returned false for existing value")
	assert.True(t, c.Check("c"), "check returned false for existing value")
	assert.True(t, c.Check("d"), "check returned false for existing value")
	assert.True(t, c.Check("e"), "check returned false for existing value")

	c.UncacheMany([]string{"a", "b", "c"})

	assert.False(t, c.Check("a"), "check returned true for uncached value")
	assert.False(t, c.Check("b"), "check returned true for uncached value")
	assert.False(t, c.Check("c"), "check returned true for uncached value")
	assert.True(t, c.Check("d"), "check returned false for existing value")
	assert.True(t, c.Check("e"), "check returned false for existing value")

}

func TesTimeoutCache_DuplicateInserts(t *testing.T) {
	c := NewTimeoutCache(4)

	c.Insert("a", time.Second)
	c.Insert("b", time.Second)
	c.Insert("b", time.Second)
	c.Insert("c", time.Second)
	assert.Equal(t, ALREADY_EXISTS, c.Insert("c", time.Second), "cache insert of duplicate didn't return error")
	assert.Equal(t, 3, c.records, "cache deduplication failed")
}

func TestTimeoutCache_ScanRate(t *testing.T) {
	c := NewTimeoutCache(-1, ScanRate(time.Millisecond*50))

	err := c.Insert("a", time.Millisecond*100)
	assert.Nil(t, err, "insert returned error")
	assert.True(t, c.Check("a"), "check returned false for existing value")

	time.Sleep(time.Millisecond * 120)
	assert.False(t, c.Check("a"), "check returned true for evicted value")
}
