package common

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/raft-kv-store/raftpb"
	log "github.com/sirupsen/logrus"
	"github.com/subchen/go-trylock/v2"
)

const LongTimeOut = 100 * time.Microsecond

type Value struct {
	k    string // For debug purpose
	V    interface{}
	mu   trylock.TryLocker
	temp bool
	txid string
}

func NewValue(k string, v interface{}) *Value {
	return &Value{
		k:  k,
		V:  v,
		mu: trylock.New(),
	}
}

func TempNewValue(k string, v interface{}) *Value {
	return &Value{
		k:    k,
		V:    v,
		mu:   trylock.New(),
		temp: true,
	}
}

type Cmap struct {
	Map     map[string]*Value
	mu      trylock.TryLocker
	timeout time.Duration
	log     *log.Entry
}

func NewCmap(logger *log.Logger, t time.Duration) *Cmap {
	l := logger.WithField("component", "cmap")
	return &Cmap{
		Map:     make(map[string]*Value),
		mu:      trylock.New(),
		timeout: t,
		log:     l,
	}
}

func NewCmapFromMap(logger *log.Logger, m map[string]interface{}, t time.Duration) *Cmap {
	l := logger.WithField("component", "cmap")
	res := &Cmap{
		Map:     make(map[string]*Value),
		mu:      trylock.New(),
		timeout: t,
		log:     l,
	}
	for k, v := range m {
		res.Map[k] = NewValue(k, v)
	}
	return res
}

func (c *Cmap) Snapshot() map[string]interface{} {
	res := make(map[string]interface{})
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, v := range c.Map {
		v.mu.RLock()
		res[k] = v.V
		v.mu.RUnlock()
	}
	return res
}

func (c *Cmap) Get(k string) (val interface{}, ok bool, err error) {
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

// MGet is multiple get
// inexistence and error are put together
func (c *Cmap) MGet(ops []*raftpb.Command) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	if global := c.mu.RTryLockTimeout(c.timeout); !global {
		return res, errors.New("map is locked globally")
	}
	defer c.mu.RUnlock()
	for _, op := range ops {
		if op.Method != GET {
			return nil, fmt.Errorf("invalid operation %v", op)
		}
		value, ok := c.Map[op.Key]
		if !ok {
			return nil, fmt.Errorf("Key=%s does not exist", op.Key)
		} else if local := value.mu.RTryLockTimeout(c.timeout); !local {
			return nil, fmt.Errorf("map is locked on Key=%s", op.Key)
		}
		res[op.Key] = value.V
		value.mu.RUnlock()
	}
	return res, nil
}

func (c *Cmap) benchmarkSet(k string, v, v0 interface{}, t time.Duration) error {
	if global := c.mu.TryLockTimeout(c.timeout); !global {
		return errors.New("map is locked globally")
	}
	value, ok := c.Map[k]
	if !ok {
		c.Map[k] = NewValue(k, v)
		c.mu.Unlock() // unlock globally asap
		return nil
	} else if local := value.mu.TryLockTimeout(c.timeout); !local {
		c.mu.Unlock() // unlock globally asap
		return fmt.Errorf("map is locked on Key=%s", k)
	}
	c.mu.Unlock()
	defer value.mu.Unlock()
	time.Sleep(t)
	if v0 != nil && value.V != v0 {
		return fmt.Errorf("condition not satisfied on Key=%s", k)
	}
	value.V = v
	return nil
}

func (c *Cmap) Set(k string, v interface{}) error {
	return c.benchmarkSet(k, v, nil, 0)
}

func (c *Cmap) SetCond(k string, v, v0 interface{}) error {
	return c.benchmarkSet(k, v, v0, 0)
}

func (c *Cmap) Del(k string) error {
	if global := c.mu.TryLockTimeout(c.timeout); !global {
		return errors.New("map is locked globally")
	}
	value, ok := c.Map[k]
	if !ok {
		c.mu.Unlock() // unlock globally asap
		return nil
	} else if local := value.mu.TryLockTimeout(c.timeout); !local { // Not to del if the key is locked by other op
		c.mu.Unlock() // unlock globally asap
		return fmt.Errorf("map is locked on Key=%s", k)
	}
	delete(c.Map, k)
	c.mu.Unlock()
	return nil
}

func (c *Cmap) TryLocks(ops []*raftpb.Command, txid string) error {
	if len(ops) == 0 {
		return errors.New("no key given")
	}
	if global := c.mu.TryLockTimeout(c.timeout); !global {
		return errors.New("map is locked globally")
	}
	// locked is used to revert lock if any trylock fails
	var locked []*Value
	var revert, cond bool
	// tmpMap is the local temp map for new value initialization
	tmpMap := make(map[string]*Value)
	for _, op := range ops {
		k := op.Key
		value, ok := c.Map[k]
		if !ok {
			// Handle new values
			// Put temp flag to delete if abort
			value = TempNewValue(k, nil)
			tmpMap[k] = value
		}
		// trylock on each value including new init
		if local := value.mu.TryLockTimeout(c.timeout); !local {
			revert = true
			break
		} else {
			locked = append(locked, value)
			// revert all locks if condition fails
			if op.Method == SET && op.Cond != nil && op.Cond.Value != value.V {
				revert = true
				cond = true
				break
			}
		}
	}
	// Link tmpMap to c.Map if no failure
	if len(tmpMap) > 0 && !revert {
		for k, v := range tmpMap {
			c.log.Infof("try lock for new key %s", k)
			c.Map[k] = v
		}
	}
	c.mu.Unlock()
	// Revert lock if failure
	if revert {
		for _, value := range locked {
			value.mu.Unlock()
		}
		if cond {
			return errors.New("set condition fails")
		}
		return errors.New("map is locked locally")
	} else {
		for _, value := range locked {
			value.txid = txid
			c.log.Infof("LOCKED for key %s in %s", value.k, value.txid)
		}
	}
	return nil
}

func (c *Cmap) WriteWithLocks(ops []*raftpb.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		switch op.Method {
		case SET:
			val, ok := c.Map[op.Key]
			if !ok {
				c.log.Fatalf("%s does not exist", op.Key)
			}
			val.V = op.Value
			// unset temp flag for committed keys
			val.temp = false
			val.mu.Unlock()
		case DEL:
			delete(c.Map, op.Key)
		default:
			c.log.Fatalf("Unknown op: %s", op.Method)
		}
	}
}

func (c *Cmap) Write(ops []*raftpb.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		switch op.Method {
		case SET:
			c.Map[op.Key] = NewValue(op.Key, op.Value)
		case DEL:
			delete(c.Map, op.Key)
		default:
			c.log.Fatalf("Unknown op: %s", op.Method)
		}
	}
}

func (c *Cmap) AbortWithLocks(ops []*raftpb.Command, txid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		val, ok := c.Map[op.Key]
		if !ok {
			continue
		}
		if val.txid != txid {
			c.log.Infof("txid CHANGE to %s != %s when trying to abort %v", val.txid, txid, ops)
		}
		if val.temp {
			// delete key is temp when aborting
			delete(c.Map, op.Key)
		}
		val.mu.TryLockTimeout(LongTimeOut)
		val.mu.Unlock()
		c.log.Infof("txid %s UNLOCK when trying to abort %v", txid, ops)
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

func (c *naiveMap) benchmarkSet(k string, v, _ interface{}, t time.Duration) error {
	c.Lock()
	defer c.Unlock()
	time.Sleep(t)
	c.Map[k] = v
	return nil
}

func (c *naiveMap) Set(k string, v interface{}) error {
	return c.benchmarkSet(k, v, nil, 0)
}

type ConcurrentMap interface {
	Get(string) (val interface{}, ok bool, err error)
	Set(string, interface{}) error
	benchmarkSet(string, interface{}, interface{}, time.Duration) error
}

func (c *Cmap) Debug(log *log.Entry, s string, k ...string) {
	if c.mu.TryLockTimeout(10) {
		c.log.Debugf("store NOT LOCKED in %s ", s)
		c.mu.Unlock()
		for _, key := range k {
			if _, ok := c.Map[key]; ok {
				if c.Map[key].mu.TryLockTimeout(10) {
					c.log.Debugf("store %s=%d NOT LOCKED in %s", key, c.Map["key"].V, s)
					c.Map[key].mu.Unlock()
				} else {
					c.log.Debugf("store %s=%d LOCKED in %s", key, c.Map["key"].V, s)
				}
			}
		}
	} else {
		c.log.Debugf("store LOCKED in PRE ")
	}
}
