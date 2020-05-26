package coordinator

import (
	"fmt"
	"github.com/hashicorp/raft"
	"net/rpc"

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

// Transaction atomically executes the transaction .
func (c *Coordinator) Transaction(cmds *raftpb.RaftCommand) (string, error) {

	c.log.Infof("Processing Transaction")

	txid := xid.New().String()
	gt := c.newGlobalTransaction(txid, cmds)
	numShards := len(gt.ShardToCommands)

	c.log.Infof("Starting prepare phase for txid: %s\n", txid)
	// Prepare Phase
	// Send prepare messages to all the shards involved in
	// transaction. This is a synchronous operation atm. It can
	// be asynchronous
	var prepareResponses int
	for _, shardops := range gt.ShardToCommands {
		if c.SendMessageToShard(shardops) {
			prepareResponses++
		}
	}

	c.log.Infof("[txid %s] prepared sent\n", txid)

	if prepareResponses != numShards {
		// send abort and report error.
		// abort will help release the locks
		// on the shards.
		c.log.Infof("[txid %s] Aborting\n", txid)
		gt.Phase = common.Abort
		// replicate via raft
		c.txMap[txid] = gt

		for _, shardops := range gt.ShardToCommands {
			shardops.Phase = common.Abort
			// best effort
			c.SendMessageToShard(shardops)

		}
		return txid, fmt.Errorf("transaction unsuccesfull, try again")
	}

	c.log.Infof("[txid: %s] Prepared recieved: %d Prepared Expected: %d", txid, prepareResponses, numShards)
	// c.log the prepared phase and replicate it
	gt.Phase = common.Prepared
	c.txMap[txid] = gt

	// TODO(imp):if the above replication fails via raft,
	// this usually means majority of nodes in the coordinator
	// cluster are faulty (partition or down). In that case,
	// retry on error.

	// Commit
	var commitResponses int
	for _, shardOps := range gt.ShardToCommands {
		// Replicate via Raft
		c.txMap[txid].Phase = common.Commit
		shardOps.Phase = common.Commit
		if c.SendMessageToShard(shardOps) {
			commitResponses++
		}
	}

	// c.log the commit phase and replicate it
	// TODO (not important): this can be garbage collected in a separate go-routine, once all acks have
	// been recieved
	gt.Phase = common.Committed
	c.txMap[txid] = gt

	// wait for all acks since client should complete replication as
	// well. not required.

	c.log.Infof("[txid :%s] Commit Ack recieved: %d Ack Expected: %d", txid, commitResponses, numShards)

	return txid, nil
}

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

