package common

import (
	"strconv"
	"testing"
	"time"
)

const networkLatency = 1000 * time.Nanosecond

func GetSet(m concurrentMap, finished chan struct{}) (set func(key string, value interface{}), get func(key string, value interface{})) {
	return func(key string, value interface{}) {
			for i := 0; i < 10; i++ {
				m.Get(key)
			}
			finished <- struct{}{}
		}, func(key string, value interface{}) {
			for i := 0; i < 10; i++ {
				m.benchmarkSet(key, value, networkLatency)
			}
			finished <- struct{}{}
		}
}

func BenchmarkCmapGetSetDifferent(b *testing.B) {
	m := NewCmap(0)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	m.Set("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i), "value")
		go get(strconv.Itoa(i-1), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkNaiveMapGetSetDifferent(b *testing.B) {
	m := NewNaiveMap(0)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	m.Set("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i), "value")
		go get(strconv.Itoa(i-1), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkCmapMultiInsertSame(b *testing.B) {
	m := NewCmap(0)
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkNaiveMapMultiInsertSame(b *testing.B) {
	m := NewNaiveMap(0)
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set("key", "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkCmapMultiGetSetBlock(b *testing.B) {
	m := NewCmap(0)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i%100), "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i%100), "value")
		go get(strconv.Itoa(i%100), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkNaiveMapMultiGetSetBlock(b *testing.B) {
	m := NewNaiveMap(0)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i%100), "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go set(strconv.Itoa(i%100), "value")
		go get(strconv.Itoa(i%100), "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}