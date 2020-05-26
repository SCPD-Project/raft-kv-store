package common

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/raft-kv-store/raftpb"
	"github.com/subchen/go-trylock/v2"
)

type Value struct {
	V  interface{}
	mu trylock.TryLocker
}

func NewValue(v interface{}) *Value {
	return &Value{
		V:  v,
		mu: trylock.New(),
	}
}

type cmap struct {
	Map     map[string]*Value
	mu      trylock.TryLocker
	timeout time.Duration
}

func NewCmap(t time.Duration) *cmap {
	return &cmap{
		Map:     make(map[string]*Value),
		mu:      trylock.New(),
		timeout: t,
	}
}

func (c *cmap) Get(k string) (val interface{}, ok bool, err error) {
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

func (c *cmap) benchmarkSet(k string, v interface{}, t time.Duration) error {
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

func (c *cmap) Set(k string, v interface{}) error {
	return c.benchmarkSet(k, v, 0)
}

func (c *cmap) TryLocks(ops []*raftpb.Command) error {
	if len(ops) == 0 {
		return errors.New("no key given")
	}
	if global := c.mu.TryLockTimeout(c.timeout); !global {
		return errors.New("map is locked globally")
	}
	// locked is used to revert lock if any trylock fails
	var locked []*Value
	var revert bool
	// tmpMap is the local temp map for new value initialization
	tmpMap := make(map[string]*Value)
	for _, op := range ops {
		k := op.Key
		value, ok := c.Map[k]
		if !ok {
			value = NewValue(nil)
			tmpMap[k] = value
		}
		// trylock on each value including new init
		if local := value.mu.TryLockTimeout(c.timeout); !local {
			revert = true
			break
		} else {
			locked = append(locked, value)
		}
	}
	// Link tmpMap to c.Map if no failure
	if len(tmpMap) > 0 && !revert {
		for k, v := range tmpMap {
			c.Map[k] = v
		}
	}
	c.mu.Unlock()
	// Revert lock if failure
	if revert {
		for _, value := range locked {
			value.mu.Unlock()
		}
		return errors.New("map is locked locally")
	}
	return nil
}

func (c *cmap) WriteWithLocks(ops []*raftpb.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		switch op.Method {
		case SET:
			c.Map[op.Key].V = op.Value
			c.Map[op.Key].mu.Unlock()
		case DEL:
			delete(c.Map, op.Key)
		default:
			log.Fatalf("Unknown op: %s", op.Method)
		}
	}
}

type naiveMap struct {
	Map map[string]interface{}
	sync.RWMutex
	timeout time.Duration
}

func NewNaiveMap(t time.Duration) *naiveMap {
	return &naiveMap{
		Map:     make(map[string]interface{}),
		timeout: t,
	}
}

func (c *naiveMap) Get(k string) (val interface{}, ok bool, err error) {
	c.RLock()
	defer c.RUnlock()
	val, ok = c.Map[k]
	return val, ok, nil
}

func (c *naiveMap) benchmarkSet(k string, v interface{}, t time.Duration) error {
	c.Lock()
	defer c.Unlock()
	time.Sleep(t)
	c.Map[k] = v
	return nil
}

func (c *naiveMap) Set(k string, v interface{}) error {
	return c.benchmarkSet(k, v, 0)
}

type concurrentMap interface {
	Get(string) (val interface{}, ok bool, err error)
	Set(string, interface{}) error
	benchmarkSet(string, interface{}, time.Duration) error
}
