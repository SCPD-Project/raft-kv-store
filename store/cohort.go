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
	store *Store

	// TODO: raft instance to replicate fsm of cohort
	cohortRaft *raft.Raft

	// replicate cohort state
	// txid -> tx cmds for this cohort(shard)
	// TODO come up with a better name
	opsMap map[string]*common.ShardOps
	// TODO use lock per key
	mu sync.Mutex
}

func startCohort(store *Store, listenAddress string) {

	c := &Cohort{
		store:  store,
		opsMap: make(map[string]*common.ShardOps),
	}
	rpc.Register(c)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
	c.store.log.Infof("RPC server started successfully on :%s", listenAddress)

	// setup raft for cohort
}

// ProcessCommands will process simple Get/Set (non-transactional) cmds from
// the coordinator.
func (c *Cohort) ProcessCommands(raftCommand *raftpb.RaftCommand, reply *common.RPCResponse) error {

	// No need to go to raft for Get/Leader cmds
	c.store.log.Info("Processing rpc call", raftCommand)
	if len(raftCommand.Commands) == 1 {
		command := raftCommand.Commands[0]
		switch command.Method {
		case raftpb.GET:
			*reply = common.RPCResponse{
				Status: 0,
				Value:  c.store.kv[command.Key],
			}
			return nil

		case raftpb.LEADER:

			if c.store.raft.State() == raft.Leader {
				*reply = common.RPCResponse{
					Status: 0,
					Value:  c.store.rpcAddress,
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

	f := c.store.raft.Apply(b, common.RaftTimeout)

	return f.Error()
}

// ProcessTransactionMessages processes prepare/commit messages from the co-ordinator.
func (c *Cohort) ProcessTransactionMessages(ops *common.ShardOps, reply *common.RPCResponse) error {

	c.store.log.Infof("Processing Transaction message :%v :%v", ops.Phase, ops.Cmds)
	switch ops.Phase {
	case common.Prepare:
		// If transaction is already in progress, prepare should return "No"
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.store.transactionInProgress {
			*reply = common.RPCResponse{
				// TODO: use different status codes to give appropriate
				// error to co-ordinator. For now, -1 is failure
				Status: -1,
				Value:  string(common.NotPrepared),
			}
			// no need to update cohort state machine, it is equivalent
			// to a no transaction.
			return fmt.Errorf("Not prepared")
		}

		// below should be raft call, once raft is setup.
		ops.Phase = common.Prepared
		// This should be replicated via raft with raft Apply, once setup
		// if raft fails, send NotPrepared. log 2 pc message
		c.opsMap[ops.Txid] = ops

		// lock is held above
		c.store.transactionInProgress = true
		*reply = common.RPCResponse{
			Status: 0,
			Value:  string(common.Prepared),
		}

	case common.Commit:

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.opsMap[ops.Txid].Phase != common.Prepared {
			// this should never happen
			*reply = common.RPCResponse{
				Status: -1,
				Value:  string(common.Invalid),
			}
			return fmt.Errorf("invalid state")
		}

		// log 2 pc commit message, replicate via raft
		ops.Phase = common.Committed
		c.opsMap[ops.Txid] = ops

		// Apply to kv
		b, err := proto.Marshal(ops.Cmds)
		if err != nil {
			return err
		}

		if f := c.store.raft.Apply(b, common.RaftTimeout); f.Error() != nil {
			// if this happens, we cannot abort the transaction at this stage. It means
			// this shard does not have a majority of replicas
			c.store.log.Warnf("Unable to apply operations to kv raft instance: %s", f.Error())
		} else {
			// release the locks
			c.store.transactionInProgress = false
		}
		*reply = common.RPCResponse{
			Status: 0,
			Value:  string(common.Committed),
		}

	case common.Abort:
		c.mu.Lock()
		defer c.mu.Unlock()

		// log 2 pc abort message, replicate via raft
		// this should be set operation on raft
		c.opsMap[ops.Txid].Phase = common.Abort
		c.store.transactionInProgress = false
		*reply = common.RPCResponse{
			Status: 0,
			Value:  string(common.Aborted),
		}

	}
	return nil
}

// ProcessJoin processes join message.
// TODO: Use this to join cohort raft as well.
func (c *Cohort) ProcessJoin(joinMsg *raftpb.JoinMsg, reply *common.RPCResponse) error {

	return c.store.Join(joinMsg.ID, joinMsg.RaftAddress)
}
