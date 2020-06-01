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
		c.store.log.Errorf("Unexpected cmd %+v", raftCommand)
	}
	command := raftCommand.Commands[0]
	switch command.Method {
	case common.GET:
		if val, ok, err := c.store.kv.Get(command.Key); ok && err == nil {
			*reply = raftpb.RPCResponse{
				Status: 0,
				Value:  val.(int64),
			}
			return nil
		} else if !ok {
			return fmt.Errorf("Key=%s does not exist", command.Key)
		} else {
			return err
		}
	case common.LEADER:
		if c.store.raft.State() == raft.Leader {
			*reply = raftpb.RPCResponse{
				Status: 0,
				Addr:   c.store.rpcAddress,
			}
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
		return err
	}

	resp, ok := applyFuture.Response().(*FSMApplyResponse)
	*reply = resp.reply
	if ok && resp.err != nil {
		return resp.err
	}

	return nil
}

// ProcessTransactionMessages processes prepare/commit messages from the coordinator.
func (c *Cohort) ProcessTransactionMessages(ops *raftpb.ShardOps, reply *raftpb.RPCResponse) error {
	c.store.log.Infof("Processing Transaction message :%v :%v", ops.Phase, ops.Cmds)
	switch ops.Phase {
	case common.Prepare:
		//Note that the transaction is read-only, iff it is read-only across all shards.
		if ops.ReadOnly {
			return c.ProcessReadOnly(ops, reply)
		}

		// This should be replicated via raft with raft Apply, once setup
		// if raft fails, send NotPrepared. log 2 pc message
		c.opsMap[ops.Txid] = ops

		if err := c.store.kv.TryLocks(ops.Cmds.Commands); err != nil {
			// If it fails to get some of the lock, prepare should return "No"
			// no need to update cohort state machine, it is equivalent to a no transaction.
			return err
		}
		*reply = raftpb.RPCResponse{
			Status: 0,
			Phase:  common.Prepared,
		}
		// below should be
		//raft call, once raft is setup.
		ops.Phase = common.Prepared


	case common.Commit:
		if c.opsMap[ops.Txid].Phase != common.Prepared {
			// this should never happen
			return errors.New("invalid state")
		}

		// log 2 pc commit message, replicate via raft
		ops.Phase = common.Committed
		c.opsMap[ops.Txid] = ops

		//Apply to fsm
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

func (c *Cohort) ProcessReadOnly(ops *raftpb.ShardOps, reply *raftpb.RPCResponse) error {
	m, err := c.store.kv.MGet(ops.Cmds.Commands)
	if err != nil {
		return err
	}
	var res []*raftpb.Command
	for k, v := range m {
		res = append(res, &raftpb.Command{Key: k, Value: v.(int64)})
	}
	*reply = raftpb.RPCResponse{
		Status:   0,
		Phase:    common.Prepared,
		Commands: res,
	}
	return nil
}


// ProcessJoin processes join message.
// TODO: Use this to join cohort raft as well.
func (c *Cohort) ProcessJoin(joinMsg *raftpb.JoinMsg, reply *raftpb.RPCResponse) error {
	return c.store.Join(joinMsg.ID, joinMsg.RaftAddress)
}

