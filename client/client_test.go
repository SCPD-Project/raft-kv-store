package client

import (
	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/raftpb"
	"reflect"
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
	test3ExpectKeys := []string{"a", "b"}
	if len(test3TxnCmds) != 2 {
		t.Errorf("Expected map size == 2 where as got : %d", len(test3TxnCmds))
	} else {
		for idx, val := range test3TxnCmds {
			// Ensure ordering of txns
			if test3ExpectKeys[idx] != val.Key {
				t.Errorf("Expected key: %s found", val.Key)
			}
		}
	}

	// del a, set a 10, set b 10, del b => del a, set a 10
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.DEL, "a", "")
	c.appendTestCmds(raftpb.SET, "a", "10")
	test4TxnCmds := c.OptimizeTxnCommands().Commands
	test4CmdSeq := []string{"del", "set"}
	if len(test4TxnCmds) != 2 {
		t.Errorf("Expected map size == 2 where as got : %d", len(test4TxnCmds))
		for idx, val := range test4TxnCmds {
			// Ensure ordering of txns
			if val.Command != test4CmdSeq[idx] {
				t.Errorf("Expected key: %s not found", val.Key)
			}
		}
	}

	// del a, del b, set a 5, set b 6, set b 15, del c => del a, del b, set a 5, set b 15, del c
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.DEL, "a", "")
	c.appendTestCmds(raftpb.DEL, "b", "")
	c.appendTestCmds(raftpb.SET, "a", "5")
	c.appendTestCmds(raftpb.SET, "b", "6")
	c.appendTestCmds(raftpb.SET, "b", "15")
	c.appendTestCmds(raftpb.DEL, "c", "")
	test5TxnCmds := c.OptimizeTxnCommands().Commands

	// for validating order of txns
	test5ExpectedOutputCmds := []httpd.TxnCommand {
		{
			Command: raftpb.DEL,
			Key:     "a",
			Value:   "",
		},
		{
			Command: raftpb.SET,
			Key:     "a",
			Value:   "5",
		},
		{
			Command: raftpb.SET,
			Key:     "b",
			Value:   "15",
		},
		{
			Command: raftpb.DEL,
			Key:     "c",
			Value:   "",
		},
	}

	if len(test5TxnCmds) != 5 {
		t.Errorf("Expected map size == 5 where as got : %d", len(test4TxnCmds))
		if !reflect.DeepEqual(test5TxnCmds, test5ExpectedOutputCmds) {
			t.Errorf("Order of txns not guaranteed")
		}
	}

	// del a => del a
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.DEL, "a", "")
	test6TxnCmds := c.OptimizeTxnCommands().Commands
	test6ExpectedKey := "del"
	if len(test6TxnCmds) != 1 {
		t.Errorf("Expected map size == 1 where as got : %d", len(test6TxnCmds))
		for _, val := range test6TxnCmds {
			// Ensure ordering of txns
			if val.Command != test6ExpectedKey {
				t.Errorf("Expected key: %s not found", val.Key)
			}
		}
	}

	// set a 5, set a 20, set a 25, set b 30 => set a 25, set b 30
	c.txnCmds = httpd.TxnJSON{}
	c.appendTestCmds(raftpb.SET, "a", "5")
	c.appendTestCmds(raftpb.SET, "a", "20")
	c.appendTestCmds(raftpb.SET, "a", "25")
	c.appendTestCmds(raftpb.SET, "b", "30")
	test7TxnCmds := c.OptimizeTxnCommands().Commands
	test7ExpectedOutputCmds := []httpd.TxnCommand {
		{
			Command: raftpb.SET,
			Key:     "a",
			Value:   "25",
		},
		{
			Command: raftpb.SET,
			Key:     "b",
			Value:   "30",
		},
	}
	if len(test7TxnCmds) != 2 {
		t.Errorf("Expected map size == 2 where as got : %d", len(test7TxnCmds))
		if !reflect.DeepEqual(test7TxnCmds, test7ExpectedOutputCmds) {
			t.Errorf("Order of txns not guaranteed")
		}
	}

}
