package common

import (
	"errors"
	"fmt"
	"github.com/subchen/go-trylock/v2"
	"sync"
	"time"
)

type Value struct{
	V interface{}
	mu trylock.TryLocker
}

func NewValue(v interface{}) *Value{
	return &Value{
		V: v,
		mu: trylock.New(),
	}
}

type cmap struct {
	Map map[string]*Value
	mu trylock.TryLocker
	timeout time.Duration
}

func NewCmap(t time.Duration) *cmap {
	return &cmap{
		Map: make(map[string]*Value),
		mu: trylock.New(),
		timeout: t,
	}
}

func (c *cmap) Get(k string) (val interface{}, ok bool, err error){
	if global := c.mu.RTryLockTimeout(c.timeout); !global {
		return val, ok, errors.New("map is locked globally")
	}
	value, ok := c.Map[k]
	if !ok {
		c.mu.RUnlock() // unlock globally asap
		return val, ok, nil
	} else if local := value.mu.RTryLockTimeout(c.timeout); !local {
		c.mu.RUnlock() // unlock globally asap
		return val, ok, fmt.Errorf("map is locked on Key=%s", k)
	}
	c.mu.RUnlock()
	defer value.mu.RUnlock()
	return value.V, ok, nil
}

func (c *cmap) benchmarkSet(k string, v interface{}, t time.Duration) error{
	if global := c.mu.TryLockTimeout(c.timeout); !global {
		return errors.New("map is locked globally")
	}
	value, ok := c.Map[k]
	if !ok {
		c.Map[k] = NewValue(v)
		c.mu.Unlock() // unlock globally asap
		return nil
	} else if local := value.mu.TryLockTimeout(c.timeout); !local {
		c.mu.Unlock() // unlock globally asap
		return fmt.Errorf("map is locked on Key=%s", k)
	}
	c.mu.Unlock()
	defer value.mu.Unlock()
	time.Sleep(t)
	value.V = v
	return nil
}

func (c *cmap) Set(k string, v interface{}) error{
	return c.benchmarkSet(k, v, 0)
}

type naiveMap struct {
	Map map[string]interface{}
	sync.RWMutex
	timeout time.Duration
}

func NewNaiveMap(t time.Duration) *naiveMap {
	return &naiveMap{
		Map: make(map[string]interface{}),
		timeout: t,
	}
}

func (c *naiveMap) Get(k string) (val interface{}, ok bool, err error){
	c.RLock()
	defer c.RUnlock()
	val, ok = c.Map[k]
	return val, ok, nil
}

func (c *naiveMap) benchmarkSet(k string, v interface{}, t time.Duration) error{
	c.Lock()
	defer c.Unlock()
	time.Sleep(t)
	c.Map[k] = v
	return nil
}

func (c *naiveMap) Set(k string, v interface{}) error{
	return c.benchmarkSet(k, v, 0)
}

type concurrentMap interface {
	Get(string) (val interface{}, ok bool, err error)
	Set(string, interface{}) error
	benchmarkSet(string, interface{}, time.Duration) error
}
