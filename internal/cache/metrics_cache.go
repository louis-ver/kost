package cache

import (
	"sync"
	"time"
)

type MetricsCache struct {
	mu           sync.RWMutex
	queueDepth   int64
	queueFetched time.Time
	spotPrice    float64
	priceFetched time.Time
}

func New() *MetricsCache {
	return &MetricsCache{}
}

func (c *MetricsCache) SetQueueDepth(depth int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueDepth = depth
	c.queueFetched = time.Now()
}

func (c *MetricsCache) QueueDepth() (depth int64, age time.Duration, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.queueFetched.IsZero() {
		return 0, 0, false
	}
	return c.queueDepth, time.Since(c.queueFetched), true
}

func (c *MetricsCache) SetSpotPrice(price float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spotPrice = price
	c.priceFetched = time.Now()
}

func (c *MetricsCache) SpotPrice() (price float64, age time.Duration, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.priceFetched.IsZero() {
		return 0, 0, false
	}
	return c.spotPrice, time.Since(c.priceFetched), true
}
