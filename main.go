package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

const hb = 1

type node struct {
	id     uint64
	ctx    context.Context
	pstore map[string]string
	store  *raft.MemoryStorage
	cfg    *raft.Config
	raft   raft.Node
	ticker <-chan time.Time
	done   <-chan struct{}
}

func newNode(id uint64, peers []raft.Peer) *node {
	store := raft.NewMemoryStorage()
	n := &node{
		id:    id,
		ctx:   context.TODO(),
		store: store,
		cfg: &raft.Config{
			ID:              id,
			ElectionTick:    10 * hb,
			HeartbeatTick:   hb,
			Storage:         store,
			MaxSizePerMsg:   math.MaxUint16,
			MaxInflightMsgs: 256,
		},
		pstore: make(map[string]string),
		ticker: time.Tick(time.Second),
		done:   make(chan struct{}),
	}

	n.raft = raft.StartNode(n.cfg, peers)
	go n.run()
	return n
}

func (n *node) run() {
	for {
		select {
		case <-n.ticker:
			n.raft.Tick()
		case rd := <-n.raft.Ready():
			n.store.Append(rd.Entries)
			n.send(rd.Messages)
			for _, entry := range rd.CommittedEntries {
				n.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					n.raft.ApplyConfChange(cc)
				}
			}
			n.raft.Advance()
		case <-n.done:
			return
		}
	}
}

func (n *node) send(messages []raftpb.Message) {
	for _, m := range messages {
		log.Println(raft.DescribeMessage(m, nil))
		// send message to other node
		nodes[int(m.To)].receive(n.ctx, m)
	}
}

func (n *node) process(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		cc.Unmarshal(entry.Data)
		//log.Printf("Term %d Index %d %s %+v", entry.Term, entry.Index, entry.Type, cc)
	}
	if entry.Type == raftpb.EntryNormal {
		if entry.Data != nil {
			parts := bytes.SplitN(entry.Data, []byte(":"), 2)
			log.Printf("Term %d Index %d %s {%s : %s}", entry.Term, entry.Index, entry.Type, string(parts[0]), string(parts[1]))
			n.pstore[string(parts[0])] = string(parts[1])
		} else {
			log.Printf("Term %d Index %d %s nil", entry.Term, entry.Index, entry.Type)
		}
	}
}

func (n *node) receive(ctx context.Context, message raftpb.Message) {
	n.raft.Step(ctx, message)
	fmt.Printf("node %d's store %v\n", n.id, n.pstore)
}

var (
	nodes = make(map[int]*node)
)

func main() {

	nodes[1] = newNode(1, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	nodes[2] = newNode(2, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	nodes[3] = newNode(3, []raft.Peer{})

	nodes[2].raft.ProposeConfChange(nodes[2].ctx, raftpb.ConfChange{
		ID:      3,
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  3,
		Context: []byte(""),
	})

	nodes[1].raft.Propose(nodes[2].ctx, []byte("mykey1:myvalue1"))
	nodes[2].raft.Propose(nodes[1].ctx, []byte("mykey2:myvalue2"))
	nodes[3].raft.Propose(nodes[3].ctx, []byte("mykey3:myvalue3"))

	time.Sleep(3000 * time.Millisecond)
	fmt.Printf("******* Finished sleeping to visualize heartbeat between nodes *******\n")

	// Just check that data has been persisted
	for i, node := range nodes {
		fmt.Printf("** Node %v **\n", i)
		for k, v := range node.pstore {
			fmt.Printf("%v = %v\n", k, v)
		}
		fmt.Printf("*************\n")
	}
}
