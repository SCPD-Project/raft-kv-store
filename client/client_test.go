package client

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	"github.com/stretchr/testify/assert"
)

const coordAddr = "127.0.0.1:17000"

func (c *RaftKVClient) appendTestCmds(method, key string, value ...int64) {
	switch method {
	case common.SET:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: method,
			Key:    key,
			Value:  value[0],
		})
	case common.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: method,
			Key:    key,
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

func TestMultiClientsMultiGet(t *testing.T) {
	var success int32
	var clients []*RaftKVClient
	for i := 0; i < 300; i++ {
		clients = append(clients, NewRaftKVClient(coordAddr))
	}
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(c *RaftKVClient) {
			defer wg.Done()
			if err := c.Get(strconv.Itoa(i)); err == nil || err.Error() == fmt.Sprintf("Key=%d does not exist", i) {
				atomic.AddInt32(&success, 1)
			} else {
				fmt.Println(err.Error())
			}
		}(c)
	}
	wg.Wait()
	fmt.Println(success)
}

func TestMultiClientsMultiSet(t *testing.T) {
	c := NewRaftKVClient(coordAddr)
	c.Delete("x")
	c.Delete("y")
	var clients []*RaftKVClient
	for i := 0; i < 5; i++ {
		clients = append(clients, NewRaftKVClient(coordAddr))
		clients[i].txnCmds = &raftpb.RaftCommand{
			Commands: []*raftpb.Command{
				{Method: common.SET, Key: "x", Value: int64(i + 1)},
				{Method: common.SET, Key: "y", Value: int64(-i - 1)},
			},
			IsTxn: true,
		}
	}
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *RaftKVClient) {
			defer wg.Done()
			_, err := c.Transaction()
			fmt.Println(err)
		}(c)
	}
	wg.Wait()
	log.Println(c.Get("y"))
}

func TestMultiXfer(t *testing.T) {
	c := NewRaftKVClient(coordAddr)
	c.Set("x", 1000)
	c.Set("y", 0)
	var clients []*RaftKVClient
	for i := 0; i < 50; i++ {
		clients = append(clients, NewRaftKVClient(coordAddr))
	}
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *RaftKVClient) {
			defer wg.Done()
			err := c.TransferTransaction([]string{common.TRANSFER, "x", "y", "1"})
			fmt.Println(err)
		}(c)
	}
	wg.Wait()
	log.Println(c.Get("y"))
}
