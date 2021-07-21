package ha_singlegroup

import (
	"fmt"
	"sync"
	"time"
)

type HACache struct {
	name        string
	ttl         time.Duration
	expireAt    time.Time
	refreshing  bool
	content     interface{}
	queryDB     func(interface{}) (interface{}, error)
	refreshSig  chan interface{}
	initArg     interface{}
	init        chan struct{}
	initialized bool
	sync.Once
	sync.Mutex
}

func NewCache(name string, ttl time.Duration, queryDb func(interface{}) (interface{}, error), initArg ...interface{}) *HACache {
	c := &HACache{
		name:       name,
		ttl:        ttl,
		queryDB:    queryDb,
		refreshSig: make(chan interface{}),
		init:       make(chan struct{}),
	}
	if len(initArg) > 0 {
		c.initArg = initArg[0]
	}
	go c.start()
	return c
}

func (c *HACache) expired() bool {
	return time.Now().After(c.expireAt)
}

func (c *HACache) Get(arg ...interface{}) interface{} {
	c.Lock()
	defer c.Unlock()

	if !c.expired() {
		return c.content
	}

	if !c.initialized {
		<-c.init
		return c.content
	}

	if !c.refreshing {
		c.refreshing = true
		if len(arg) > 0 {
			c.refreshSig <- arg[0]
		} else {
			c.refreshSig <- struct{}{}
		}
	}

	return c.content
}

func (c *HACache) start() {
	// init
	for {
		content, err := c.queryDB(c.initArg)
		if err != nil {
			fmt.Printf("[ERROR] HA-singlegroup init %s queryDB failed: %v\n", c.name, err)
			time.Sleep(time.Second)
		} else {
			c.content = content
			c.initialized = true
			c.expireAt = time.Now().Add(c.ttl)
			close(c.init)
			break
		}
	}

	for {
		select {
		case arg := <-c.refreshSig:
			content, err := c.queryDB(arg)
			if err != nil {
				fmt.Printf("[ERROR] HA-singlegroup %s queryDB failed: %v\n", c.name, err)
			} else {
				c.Lock()
				c.content = content
				c.refreshing = false
				c.expireAt = time.Now().Add(c.ttl)
				c.Unlock()
			}
		}
	}
}
