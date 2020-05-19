package store

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

// Cohort maintains state of the cohort state machine. It also starts
// a rpc server to listen to commands from coordinator.
type Cohort struct {
	s *Store

	// TODO: raft instance to replicate fsm of cohort
	cohortRaft *raft.Raft

	// replicate cohort state
	// txid -> tx cmds for this cohort(shard)
	cState map[string]*common.ShardOps
	m      sync.Mutex
}

func startCohort(store *Store, listenAddress string) {

	c := &Cohort{
		s:      store,
		cState: make(map[string]*common.ShardOps),
	}
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", listenAddress)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Println("RPC server started successfully")

	// setup raft for cohort
}

// ProcessCommands will process simple Get/Set (non-transactional) cmds from
// the coordinator.
func (c *Cohort) ProcessCommands(raftCommand *raftpb.RaftCommand, reply *common.RPCResponse) error {

	// No need to go to raft for Get/Leader cmds
	log.Println("Processing rpc call", raftCommand)
	if len(raftCommand.Commands) == 1 {
		command := raftCommand.Commands[0]
		switch command.Method {
		case GET:
			*reply = common.RPCResponse{
				Status: 0,
				Value:  c.s.kv[command.Key],
			}
			return nil

		case LEADER:
			log.Println("Processing leader request", string(c.s.raft.Leader()))

			if c.s.raft.State() == raft.Leader {
				*reply = common.RPCResponse{
					Status: 0,
					Value:  c.s.rpcAddress,
				}
			} else {
				*reply = common.RPCResponse{
					Status: -1,
					Value:  "",
				}
			}

			return nil
		}
	}

	b, err := proto.Marshal(raftCommand)
	if err != nil {
		return err
	}

	f := c.s.raft.Apply(b, common.RaftTimeout)

	return f.Error()
}

// ProcessTransactionMessages processes prepare/commit messages from the co-ordinator.
func (c *Cohort) ProcessTransactionMessages(ops *common.ShardOps, reply *common.RPCResponse) error {

	switch ops.MessageType {
	case common.Prepare:
		// If transaction is already in progress, prepare should return "No"
		c.m.Lock()
		defer c.m.Unlock()
		if c.s.transactionInProgress {
			*reply = common.RPCResponse{
				// TODO: use different status codes to give appropriate
				// error to co-ordinator. For now, -1 is failure
				Status: -1,
				Value:  common.NotPrepared,
			}
			// no need to update cohort state machine, it is equivalent
			// to a no transaction.
			return fmt.Errorf("Not prepared")
		}

		// below should be raft call, once raft is setup.
		ops.MessageType = common.Prepared
		// This should be replicated via raft with raft Apply, once setup
		// if raft fails, send NotPrepared. log 2 pc message
		c.cState[ops.Txid] = ops

		// lock is held above
		c.s.transactionInProgress = true
		*reply = common.RPCResponse{
			Status: 0,
			Value:  common.Prepared,
		}

	case common.Commit:

		c.m.Lock()
		defer c.m.Unlock()

		if c.cState[ops.Txid].MessageType != common.Prepared {
			// this should never happen
			*reply = common.RPCResponse{
				Status: -1,
				Value:  common.Invalid,
			}
			return fmt.Errorf("invalid state")
		}

		// log 2 pc commit message, replicate via raft
		ops.MessageType = common.Committed
		c.cState[ops.Txid] = ops

		// Apply to kv
		b, err := proto.Marshal(ops.Cmds)
		if err != nil {
			return err
		}

		f := c.s.raft.Apply(b, common.RaftTimeout)
		if f.Error() != nil {
			// if this happens, we cannot abort the transaction at this stage. It means
			// this shard does not have a majority of replicas
			log.Printf("Unable to apply operations to kv raft instance: %s", f.Error())
		} else {
			// release the locks
			c.s.transactionInProgress = false
		}

	case common.Abort:
		c.m.Lock()
		defer c.m.Unlock()

		// log 2 pc abort message, replicate via raft
		// this should be set operation on raft
		c.cState[ops.Txid].MessageType = common.Abort
		c.s.transactionInProgress = false

	}
	return nil
}
