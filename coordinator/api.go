package coordinator

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/raft"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	"github.com/rs/xid"
)

// TODO: Separate out the common code into a function

// Get returns the value for the given key.
func (c *Coordinator) Get(key string) (int64, error) {

	c.log.Infof("Processing Get request %s", key)
	var response raftpb.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: common.GET,
				Key:    key,
			},
		},
	}

	// Figure out
	addr, _, err := c.FindLeader(key)
	if err != nil {
		return 0, err
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return 0, err
	}

	err = client.Call("Cohort.ProcessCommands", cmd, &response)
	c.log.Infof(" Value of key: %s --> %d", key, response.Value)

	return response.Value, err

}

// Set sets the value for the given key.
func (c *Coordinator) Set(key string, value int64) error {

	c.log.Infof("Processing Set request: Key=%s Value=%d", key, value)
	var response raftpb.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: common.SET,
				Key:    key,
				Value:  value,
			},
		},
	}
	// Figure out
	addr, _, err := c.FindLeader(key)
	if err != nil {
		return err
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return fmt.Errorf("Unable to reach shard at :%s", addr)
	}

	return client.Call("Cohort.ProcessCommands", cmd, &response)

}

// Delete deletes the given key.
func (c *Coordinator) Delete(key string) error {

	c.log.Infof("Processing Delete request %s", key)
	var response raftpb.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: common.DEL,
				Key:    key,
			},
		},
	}

	// Figure out
	addr, _, err := c.FindLeader(key)
	if err != nil {
		return err
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return err
	}

	return client.Call("Cohort.ProcessCommands", cmd, &response)

}

func isReadOnly(ops []*raftpb.Command) bool {
	for _, op := range ops {
		if op.Method != common.GET {
			return false
		}
	}
	return true
}

// Transaction atomically executes the transaction .
func (c *Coordinator) Transaction(cmds *raftpb.RaftCommand) (*raftpb.RaftCommand, error) {

	c.log.Infof("Processing Transaction")
	txid := xid.New().String()
	gt := c.newGlobalTransaction(txid, cmds)
	readOnly := isReadOnly(gt.Cmds.Commands)
	numShards := len(gt.ShardToCommands)
	resultCmds := &raftpb.RaftCommand{}

	c.log.Infof("Starting prepare phase for txid: [%s]", txid)
	// Prepare Phase
	// Send prepare messages to all the shards involved in transaction.
	// This is a synchronous operation atm. It can be asynchronous
	// TODO: go routine SendMessageToShard
	var prepareResponses int
	var readOnlyErr error
	if readOnly {
		c.log.Infof("[txid %s] is read-only", txid)
	}
	for _, shardops := range gt.ShardToCommands {
		shardops.ReadOnly = readOnly
		cmds, err := c.SendMessageToShard(shardops)
		if err == nil {
			prepareResponses++
		} else {
			c.log.Infof("[txid %s] failed at %v with %s", txid, shardops, err.Error())
		}
		if readOnly {
			resultCmds.Commands = append(resultCmds.Commands, cmds...)
			if err != nil {
				readOnlyErr = err
			}
		}
	}
	if readOnly {
		c.log.Infof("[txid: %s] read-only transaction, returning after prepare phase", txid)
		return resultCmds, readOnlyErr
	}

	c.log.Infof("[txid %s] Prepared sent", txid)

	if prepareResponses != numShards {
		// send abort and report error.
		// abort will help release the locks
		// on the shards.
		c.log.Infof("[txid %s] Aborting\n", txid)
		gt.Phase = common.Abort
		// replicate via raft
		if err := c.Replicate(txid, common.SET, gt); err != nil {
			c.log.Infof("[txid: %s] failed to set Abort state: %s", txid, err)
		}

		var err error
		for _, shardops := range gt.ShardToCommands {
			shardops.Phase = common.Abort
			// best effort
			_, err = c.SendMessageToShard(shardops)
			if err != nil {
				c.log.Infof("[txid %s] failed at %v with %s", txid, shardops, err.Error())
			}
		}
		return nil, err
	}

	c.log.Infof("[txid: %s] Prepared recieved: %d Prepared Expected: %d", txid, prepareResponses, numShards)

	// c.log the prepared phase and replicate it
	gt.Phase = common.Prepared
	if err := c.Replicate(txid, common.SET, gt); err != nil {
		c.log.Errorf("[txid: %s] failed to set Prepared state: %s", txid, err)
		return nil, fmt.Errorf("unable to complete transaction: %s", err)
	}

	// TODO(imp):if the above replication fails via raft,
	// this usually means majority of nodes in the coordinator
	// cluster are faulty (partition or down). In that case,
	// retry on error. This will be done via clean up goroutine

	// Comment: In general, there is nothing much that can be
	// done on raft failures. A transaction recovery go-routine
	// that runs periodically (TODO) and does the following
	// 1. if transaction is not in commit state (meaning commit message not sent
	// to shards), after t seconds (some constant), send an abort and release locks
	// 2. if transaction is in commit state, keep trying to send commit and complete
	// the transaction.

	// Commit
	var commitResponses int
	for _, shardOps := range gt.ShardToCommands {
		// Replicate via Raft
		gt.Phase = common.Commit
		shardOps.Phase = common.Commit

		if err := c.Replicate(txid, common.SET, gt); err != nil {
			c.log.Errorf("[txid: %s] failed to set commit state: %s", txid, err)
			return nil, fmt.Errorf("unable to complete transaction: %s", err)
		}

		if _, err := c.SendMessageToShard(shardOps); err == nil {
			commitResponses++
		}
	}

	// c.log the commit phase and replicate it
	// TODO (not important): this can be garbage collected in a separate go-routine, once all acks have
	// been recieved
	gt.Phase = common.Committed
	if err := c.Replicate(txid, common.SET, gt); err != nil {
		c.log.Infof("[txid: %s] failed to set commited state: %s", txid, err)
		// Note: there is no need to return with an error here, At this point, the cohorts have
		// already got the commit message. From the client perspective, this transaction
		// is successfull.
	}

	// wait for all acks since client should complete replication as
	// well. not required.

	c.log.Infof("[txid :%s] Commit Ack recieved: %d Ack Expected: %d", txid, commitResponses, numShards)

	return resultCmds, nil
}

// newGlobalTransaction creates a new transaction object and returns if a transaction is
// read-only or not.
func (c *Coordinator) newGlobalTransaction(txid string, cmds *raftpb.RaftCommand) *raftpb.GlobalTransaction {

	gt := &raftpb.GlobalTransaction{
		Txid:  txid,
		Cmds:  cmds,
		Phase: common.Prepare,
	}

	shardToCmds := make(map[int64]*raftpb.ShardOps)

	for _, cmd := range cmds.Commands {
		shardID := c.GetShardID(cmd.Key)
		if _, ok := shardToCmds[shardID]; !ok {
			shardToCmds[shardID] = &raftpb.ShardOps{
				Txid:      txid,
				MasterKey: cmd.Key,
				Phase:     common.Prepare,
				Cmds: &raftpb.RaftCommand{
					Commands: []*raftpb.Command{},
					IsTxn:    cmds.IsTxn,
				},
			}
		}
		// TODO: this is short, but convoluted. split this.
		shardToCmds[shardID].Cmds.Commands = append(shardToCmds[shardID].Cmds.Commands, cmd)
	}
	gt.ShardToCommands = shardToCmds

	return gt
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
// TODO: Make an interface
func (c *Coordinator) Join(nodeID, addr string) error {
	c.log.Infof("received join request for remote node %s at %s", nodeID, addr)

	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		c.log.Errorf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				c.log.Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := c.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	c.log.Infof("node %s at %s joined successfully", nodeID, addr)
	return nil
}
