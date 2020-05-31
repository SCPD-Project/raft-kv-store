package store

import (
	"errors"
	"fmt"
	"log"

	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
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
	opsMap map[string]*raftpb.ShardOps
	// TODO use lock per key
	mu sync.Mutex
}

func startCohort(store *Store, listenAddress string) {

	c := &Cohort{
		store:  store,
		opsMap: make(map[string]*raftpb.ShardOps),
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
func (c *Cohort) ProcessCommands(raftCommand *raftpb.RaftCommand, reply *raftpb.RPCResponse) error {
	// No need to go to raft for Get/Leader cmds
	c.store.log.Info("Processing rpc call", raftCommand)
	if len(raftCommand.Commands) != 1 {
		c.store.log.Fatalf("Unexpected cmd %+v", raftCommand)
	}
	command := raftCommand.Commands[0]
	switch command.Method {
	case common.LEADER:
		if c.store.raft.State() == raft.Leader {
			*reply = raftpb.RPCResponse{
				Status: 0,
				Addr:   c.store.rpcAddress,
			}
		} else {
			*reply = raftpb.RPCResponse{
				Status: -1,
			}
		}
		return nil
	case common.GET:
		if val, ok, err := c.store.kv.Get(command.Key); ok && err == nil {
			*reply = raftpb.RPCResponse{
				Status: 0,
				Value:  val.(int64),
			}
		} else if !ok {
			*reply = raftpb.RPCResponse{
				Status: -1,
			}
			return fmt.Errorf("Key=%s does not exist", command.Key)
		} else {
			*reply = raftpb.RPCResponse{
				Status: -1,
			}
			return err
		}
		return nil
	}

	// Only Set and Del is apply to fsm
	b, err := proto.Marshal(raftCommand)
	if err != nil {
		return err
	}

	applyFuture := c.store.raft.Apply(b, common.RaftTimeout)
	if err := applyFuture.Error(); err != nil {
		*reply = raftpb.RPCResponse{
			Status: -1,
		}
		return err
	}

	if resp := applyFuture.Response().(*FSMApplyResponse); resp.err != nil {
		*reply = resp.reply
		return resp.err
	}

	return nil
}

// ProcessTransactionMessages processes prepare/commit messages from the coordinator.
func (c *Cohort) ProcessTransactionMessages(ops *raftpb.ShardOps, reply *raftpb.RPCResponse) error {

	c.store.log.Infof("Processing Transaction message :%v :%v", ops.Phase, ops.Cmds)
	switch ops.Phase {
	case common.Prepare:
		// If it fails to get some of the lock, prepare should return "No"
		if err := c.store.kv.TryLocks(ops.Cmds.Commands); err != nil {
			*reply = raftpb.RPCResponse{
				// TODO: use different status codes to give appropriate
				// error to coordinator. For now, -1 is failure
				Status: -1,
				Phase:  common.NotPrepared,
			}
			// no need to update cohort state machine, it is equivalent to a no transaction.
			return err
		}

		// below should be raft call, once raft is setup.
		ops.Phase = common.Prepared
		// This should be replicated via raft with raft Apply, once setup
		// if raft fails, send NotPrepared. log 2 pc message
		c.opsMap[ops.Txid] = ops

		*reply = raftpb.RPCResponse{
			Status: 0,
			Phase:  common.Prepared,
		}

	case common.Commit:
		if c.opsMap[ops.Txid].Phase != common.Prepared {
			// this should never happen
			*reply = raftpb.RPCResponse{
				Status: -1,
				Phase:  common.Invalid,
			}
			return errors.New("invalid state")
		}

		// log 2 pc commit message, replicate via raft
		ops.Phase = common.Committed
		c.opsMap[ops.Txid] = ops


		// Apply to fsm
		b, err := proto.Marshal(ops.Cmds)
		if err != nil {
			return err
		}
		if f := c.store.raft.Apply(b, common.RaftTimeout); f.Error() != nil {
			// if this happens, we cannot abort the transaction at this stage. It means
			// this shard does not have a majority of replicas
			c.store.log.Warnf("Unable to apply operations to kv raft instance: %s", f.Error())
		}

		*reply = raftpb.RPCResponse{
			Status: 0,
			Phase:  common.Committed,
		}

	case common.Abort:
		// log 2 pc abort message, replicate via raft
		// this should be set operation on raft
		c.opsMap[ops.Txid].Phase = common.Abort
		c.store.kv.AbortWithLocks(ops.Cmds.Commands)
		*reply = raftpb.RPCResponse{
			Status: 0,
			Phase:  common.Aborted,
		}

	}
	return nil
}

// ProcessJoin processes join message.
// TODO: Use this to join cohort raft as well.
func (c *Cohort) ProcessJoin(joinMsg *raftpb.JoinMsg, reply *raftpb.RPCResponse) error {

	return c.store.Join(joinMsg.ID, joinMsg.RaftAddress)
}
