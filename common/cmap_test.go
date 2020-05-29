package common

import (
	"fmt"
	"github.com/raft-kv-store/raftpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCmap_TryLocks(t *testing.T) {
	// TryLocks succeeds without intersected keys
	m1 := NewCmap(0)
	m1.Set("a", 1)
	m1.Set("b", 2)
	op1 := []*raftpb.Command{
		{Method: SET, Key: "c", Value: 3},
		{Method: DEL, Key: "d"},
	}
	m1.TryLocks(op1)

	assert.True(t, m1.mu.TryLockTimeout(0), "Cmap should not be globally locked")
	m1.mu.Unlock()

	for k, expected := range map[string]interface{}{"a": 1, "b": 2} {
		actual, ok, err := m1.Get(k)
		assert.Truef(t, err == nil, "no error should be expected for key %s", k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, expected := range map[string]interface{}{"c": nil, "d": nil} {
		_, ok, err := m1.Get(k)
		expectedErr := fmt.Errorf("map is locked on Key=%s", k)
		assert.Truef(t, err.Error() == expectedErr.Error(), "Expected %s, but got %s for key %s", expectedErr.Error(), err.Error(), k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		m1.Map[k].mu.Unlock()
		actual, _, _ := m1.Get(k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}

	// TryLocks succeeds with intersected keys locked
	m2 := NewCmap(0)
	m2.Set("a", 1)
	m2.Set("b", 2)
	op2 := []*raftpb.Command{
		{Method: SET, Key: "b", Value: 3},
		{Method: DEL, Key: "c"},
		{Method: SET, Key: "d", Value: 4},
	}
	m2.TryLocks(op2)

	assert.True(t, m2.mu.TryLockTimeout(0), "Cmap should not be globally locked")
	m2.mu.Unlock()

	for k, expected := range map[string]interface{}{"a": 1} {
		actual, ok, err := m2.Get(k)
		assert.Truef(t, err == nil, "no error should be expected for key %s", k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, expected := range map[string]interface{}{"b": 2} {
		actual, ok, err := m2.Get(k)
		expectedErr := fmt.Errorf("map is locked on Key=%s", k)
		assert.Truef(t, err.Error() == expectedErr.Error(), "Expected %s, but got %s for key %s", expectedErr.Error(), err.Error(), k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		m2.Map[k].mu.Unlock()
		actual, _, _ = m2.Get(k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, expected := range map[string]interface{}{"c": nil, "d": nil} {
		_, ok, err := m2.Get(k)
		expectedErr := fmt.Errorf("map is locked on Key=%s", k)
		assert.Truef(t, err.Error() == expectedErr.Error(), "Expected %s, but got %s for key %s", expectedErr.Error(), err.Error(), k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		m2.Map[k].mu.Unlock()
		actual, _, _ := m2.Get(k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}

	// TryLocks fails with intersected keys locked
	m3 := NewCmap(0)
	m3.Set("a", 1)
	m3.Set("b", 2)
	m3.Set("c", 6)
	m3.Set("d", 5)
	m3.Map["a"].mu.Lock()
	m3.Map["d"].mu.Lock()
	op3 := []*raftpb.Command{
		{Method: SET, Key: "b", Value: 3},
		{Method: DEL, Key: "a"},
		{Method: SET, Key: "c", Value: 4},
		{Method: SET, Key: "e", Value: 7},
		{Method: SET, Key: "f", Value: 8},
	}
	m3.TryLocks(op3)

	assert.True(t, m3.mu.TryLockTimeout(0), "Cmap should not be globally locked")
	m3.mu.Unlock()

	for k, expected := range map[string]interface{}{"a": 1, "d": 5} {
		actual, ok, err := m3.Get(k)
		expectedErr := fmt.Errorf("map is locked on Key=%s", k)
		assert.Truef(t, err.Error() == expectedErr.Error(), "Expected %s, but got %s for key %s", expectedErr.Error(), err.Error(), k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		m3.Map[k].mu.Unlock()
		actual, _, _ = m3.Get(k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, expected := range map[string]interface{}{"b": 2, "c": 6} {
		actual, ok, err := m3.Get(k)
		assert.Truef(t, err == nil, "Not error is expected for key %s", k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, _ := range map[string]interface{}{"e": nil, "f": nil} {
		_, ok, err := m3.Get(k)
		assert.Truef(t, err == nil, "Not error is expected for key %s", k)
		assert.Truef(t, !ok, "Value should not exist for key %s", k)
	}

	// TryLocks fails with global lock
	m4 := NewCmap(0)
	m4.Set("a", 1)
	m4.Set("b", 2)
	m4.Set("c", 6)
	m4.Set("d", 5)
	m4.Map["a"].mu.Lock()
	m4.Map["d"].mu.Lock()
	m4.mu.Lock()
	op4 := []*raftpb.Command{
		{Method: SET, Key: "b", Value: 3},
		{Method: DEL, Key: "a"},
		{Method: SET, Key: "c", Value: 4},
		{Method: SET, Key: "e", Value: 7},
		{Method: SET, Key: "f", Value: 8},
	}
	m4.TryLocks(op4)

	assert.True(t, !m4.mu.TryLockTimeout(0), "Cmap should be globally locked")
	m4.mu.Unlock()

	for k, expected := range map[string]interface{}{"a": 1, "d": 5} {
		actual, ok, err := m4.Get(k)
		expectedErr := fmt.Errorf("map is locked on Key=%s", k)
		assert.Truef(t, err.Error() == expectedErr.Error(), "Expected %s, but got %s for key %s", expectedErr.Error(), err.Error(), k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		m4.Map[k].mu.Unlock()
		actual, _, _ = m4.Get(k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, expected := range map[string]interface{}{"b": 2, "c": 6} {
		actual, ok, err := m4.Get(k)
		assert.Truef(t, err == nil, "Not error is expected for key %s", k)
		assert.Truef(t, ok, "Value should exist for key %s", k)
		assert.Equalf(t, expected, actual, "Expected %d, but got %d for key %s", expected, actual, k)
	}
	for k, _ := range map[string]interface{}{"e": nil, "f": nil} {
		_, ok, err := m4.Get(k)
		assert.Truef(t, err == nil, "Not error is expected for key %s", k)
		assert.Truef(t, !ok, "Value should not exist for key %s", k)
	}
}