package client

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	"github.com/stretchr/testify/assert"
)

func (c *raftKVClient) appendTestCmds(method, key string, value ...int64) {
	switch method {
	case common.SET:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: method,
			Key:     key,
			Value:   value[0],
		})
	case common.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: method,
			Key:     key,
		})
	}
}

func TestClient(t *testing.T) {
	c := NewRaftKVClient("localhost:20000")

	// set a 5, set a 3, set b 4, del a => set b 4
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.SET, "a", 5)
	c.appendTestCmds(common.SET, "a", 3)
	c.appendTestCmds(common.SET, "b", 4)
	c.appendTestCmds(common.DEL, "a")
	c.OptimizeTxnCommands()
	expectedCmds := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{{Method: common.SET, Key: "b", Value: 4}},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)


	// set a 5, set a 3, del a => no effect
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.SET, "a", 5)
	c.appendTestCmds(common.SET, "a", 3)
	c.appendTestCmds(common.DEL, "a")
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)


	// set a 5, set a 10, del b => set a 10, del b
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.SET, "a", 5)
	c.appendTestCmds(common.SET, "a", 10)
	c.appendTestCmds(common.DEL, "b")
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.SET, Key: "a", Value: 10},
			{Method: common.DEL, Key: "b"},
		},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)

	// del a, set a 10, set b 10, del b => del a, set a 10
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.DEL, "a")
	c.appendTestCmds(common.SET, "a", 10)
	c.appendTestCmds(common.SET, "b", 10)
	c.appendTestCmds(common.DEL, "b")
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.DEL, Key: "a"},
			{Method: common.SET, Key: "a", Value: 10},
		},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)


	// del a, del b, set a 5, set b 6, set b 15, del c => del a, del b, set a 5, set b 15, del c
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.DEL, "a")
	c.appendTestCmds(common.DEL, "b")
	c.appendTestCmds(common.SET, "a", 5)
	c.appendTestCmds(common.SET, "b", 6)
	c.appendTestCmds(common.SET, "b", 15)
	c.appendTestCmds(common.DEL, "c")
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.DEL, Key: "a"},
			{Method: common.DEL, Key: "b"},
			{Method: common.SET, Key: "a", Value: 5},
			{Method: common.SET, Key: "b", Value: 15},
			{Method: common.DEL, Key: "c"},
		},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)


	// del a => del a
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.DEL, "a")
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.DEL, Key: "a"},
		},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)


	// set a 5, set a 20, set a 25, set b 30 => set a 25, set b 30
	c.txnCmds = &raftpb.RaftCommand{}
	c.appendTestCmds(common.SET, "a", 5)
	c.appendTestCmds(common.SET, "a", 20)
	c.appendTestCmds(common.SET, "a", 25)
	c.appendTestCmds(common.SET, "b", 30)
	c.OptimizeTxnCommands()
	expectedCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.SET, Key: "a", Value: 25},
			{Method: common.SET, Key: "b", Value: 30},
		},
	}
	assert.Truef(t, cmp.Equal(expectedCmds, c.txnCmds), "Expected %v but got %v", expectedCmds, c.txnCmds)

}
