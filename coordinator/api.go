package coordinator

import (
	"fmt"
	"net/rpc"

	"github.com/rs/xid"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/raftpb"
)

// TODO: Separate out the common code into a function

// Get returns the value for the given key.
func (c *Coordinator) Get(key string) (string, error) {

	c.log.Infof("Processing Get request %s", key)
	var response common.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: raftpb.GET,
				Key:    key,
			},
		},
	}

	// Figure out
	addr, _, err := c.FindLeader(key)
	if err != nil {
		return "", err
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return "", err
	}

	err = client.Call("Cohort.ProcessCommands", cmd, &response)

	return response.Value, nil

}

// Set sets the value for the given key.
func (c *Coordinator) Set(key, value string) error {

	c.log.Infof("Processing Set request: Key=%s Value=%s", key, value)
	var response common.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: raftpb.SET,
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
		c.log.Fatal("dialing:", err)
	}

	err = client.Call("Cohort.ProcessCommands", cmd, &response)

	return nil

}

// Delete deletes the given key.
func (c *Coordinator) Delete(key string) error {

	c.log.Infof("Processing Delete request %s", key)
	var response common.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: raftpb.DEL,
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

	err = client.Call("Cohort.ProcessCommands", cmd, &response)
	return nil

}

// Transaction atomically executes the transaction .
func (c *Coordinator) Transaction(ops []*raftpb.Command) (string, error) {

	c.log.Infof("Processing Transaction")
	cmds := &raftpb.RaftCommand{
		Commands: ops,
	}

	txid := xid.New().String()
	gt := c.createGlobalTransactionObject(txid, cmds)
	numShards := len(gt.ShardToCommands)

	c.log.Infof("Starting prepare phase for txid: %s\n", txid)
	// Prepare Phase
	// Send prepare messages to all the shards involved in
	// transaction. This is a synchronous operation atm. It can
	// be asynchronous
	prepareResponses := 0
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
		c.cstate[txid] = gt

		for _, shardops := range gt.ShardToCommands {
			shardops.MessageType = common.Abort
			// best effort
			_ = c.SendMessageToShard(shardops)

		}
		return txid, fmt.Errorf("transaction unsuccesfull, try again")
	}

	c.log.Infof("[txid: %s] Prepared recieved: %d Prepared Expected: %d", txid, prepareResponses, numShards)
	// c.log the prepared phase and replicate it
	gt.Phase = common.Prepared
	c.cstate[txid] = gt

	// TODO(imp):if the above replication fails via raft,
	// this usually means majority of nodes in the coordinator
	// cluster are faulty (partition or down). In that case,
	// retry on error.

	// Commit
	commitResponses := 0
	for _, shardops := range gt.ShardToCommands {
		// Relicate via Raft
		c.cstate[txid].Phase = common.Commit
		shardops.MessageType = common.Commit
		if c.SendMessageToShard(shardops) {
			commitResponses++
		}
	}

	// c.log the commit phase and replicate it
	// TODO (not important): this can be garbage collected in a separate go-routine, once all acks have
	// been recieved
	gt.Phase = common.Committed
	c.cstate[txid] = gt

	// wait for all acks since client should complete replication as
	// well. not required.

	c.log.Infof("[txid :%s] Commit Ack recieved: %d Ack Expected: %d", txid, commitResponses, numShards)

	return txid, nil
}

func (c *Coordinator) createGlobalTransactionObject(txid string, cmds *raftpb.RaftCommand) *common.GlobalTransaction {

	gt := &common.GlobalTransaction{
		Txid:  txid,
		Cmds:  cmds,
		Phase: common.Prepare,
	}

	shardToCmds := make(map[int]*common.ShardOps)

	for _, cmd := range cmds.Commands {

		if _, ok := shardToCmds[c.GetShardID(cmd.Key)]; !ok {

			shardToCmds[c.GetShardID(cmd.Key)] = &common.ShardOps{
				Txid:        txid,
				MasterKey:   cmd.Key,
				MessageType: common.Prepare,
				Cmds: &raftpb.RaftCommand{
					Commands: []*raftpb.Command{},
				},
			}
		}
		// TODO: this is short, but convoluted. split this.
		shardToCmds[c.GetShardID(cmd.Key)].Cmds.Commands = append(shardToCmds[c.GetShardID(cmd.Key)].Cmds.Commands, cmd)
	}
	gt.ShardToCommands = shardToCmds

	return gt
}
