package cache_test

import (
	"testing"
	"time"

	"github.com/louisolivier/kost/internal/cache"
)

func TestQueueDepth_EmptyCache_ReturnsNotOk(t *testing.T) {
	c := cache.New()
	_, _, ok := c.QueueDepth()
	if ok {
		t.Fatal("expected ok=false on empty cache")
	}
}

func TestQueueDepth_AfterSet_ReturnsValue(t *testing.T) {
	c := cache.New()
	c.SetQueueDepth(42)
	depth, age, ok := c.QueueDepth()
	if !ok {
		t.Fatal("expected ok=true after set")
	}
	if depth != 42 {
		t.Errorf("expected 42, got %d", depth)
	}
	if age > time.Second {
		t.Errorf("expected age < 1s, got %v", age)
	}
}

func TestSpotPrice_EmptyCache_ReturnsNotOk(t *testing.T) {
	c := cache.New()
	_, _, ok := c.SpotPrice()
	if ok {
		t.Fatal("expected ok=false on empty cache")
	}
}

func TestSpotPrice_AfterSet_ReturnsValue(t *testing.T) {
	c := cache.New()
	c.SetSpotPrice(0.089)
	price, age, ok := c.SpotPrice()
	if !ok {
		t.Fatal("expected ok=true after set")
	}
	if price != 0.089 {
		t.Errorf("expected 0.089, got %f", price)
	}
	if age > time.Second {
		t.Errorf("expected age < 1s, got %v", age)
	}
}
