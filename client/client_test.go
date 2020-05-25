package client

import (
	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/raftpb"
	"testing"
)

func (c *raftKVClient) appendTestCmds(method, key, value string) {
	switch method {
	case raftpb.SET:
		c.txnCmds.Commands = append(c.txnCmds.Commands, httpd.TxnCommand{
		Command: method,
		Key: key,
		Value: value,
		})
	case raftpb.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, httpd.TxnCommand{
			Command: method,
			Key: key,
		})
	}
}

func TestClient(t *testing.T) {
	c := NewRaftKVClient("localhost:20000")

	// set a 5, set a 3, set b 4, del a => set b
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.SET, "a", "5")
	c.appendTestCmds(raftpb.SET, "a", "3")
	c.appendTestCmds(raftpb.SET, "b", "10")
	c.appendTestCmds(raftpb.DEL, "a", "")
	test1TxnCmds := c.OptimizeTxnCommands().Commands
	test1ExpectKey := "b"
	if len(test1TxnCmds) != 1 {
		t.Errorf("Expected map size == 1 where as got : %d", len(test1TxnCmds))
	} else {
		for _, val := range test1TxnCmds {
			if val.Key != test1ExpectKey || val.Value != "10" {
				t.Errorf("Expected key: %s not found", test1ExpectKey)
			}
		}
	}

	// set a 5, set a 3, del a => no effect
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.SET, "a", "5")
	c.appendTestCmds(raftpb.SET, "a", "3")
	c.appendTestCmds(raftpb.DEL, "a", "")
	test2TxnCmds := c.OptimizeTxnCommands().Commands
	if len(test2TxnCmds) > 0 {
		t.Errorf("Expected no txn effect where as got :%d", len(test2TxnCmds))
	}

	// set a 5, set a 10, del b => set a 10, del b
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.SET, "a", "5")
	c.appendTestCmds(raftpb.SET, "a", "10")
	c.appendTestCmds(raftpb.DEL, "b", "")
	test3TxnCmds := c.OptimizeTxnCommands().Commands
	test3ExpectKeys := map[string]string {"a": "10", "b": ""}
	if len(test3TxnCmds) != 2 {
		t.Errorf("Expected map size == 2 where as got : %d", len(test3TxnCmds))
	} else {
		for _, val := range test3TxnCmds {
			mapVal, ok := test3ExpectKeys[val.Key]; if !ok || val.Value != mapVal {
				t.Errorf("Expected key: %s not found", val.Key)
			}
		}
	}

	// del a, set a 10 => set a 10
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.DEL, "a", "5")
	c.appendTestCmds(raftpb.SET, "a", "10")
	test4TxnCmds := c.OptimizeTxnCommands().Commands
	if len(test4TxnCmds) != 1 {
		t.Errorf("Expected map size == 1 where as got : %d", len(test4TxnCmds))
	}
}