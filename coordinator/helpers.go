package coordinator

import (
	"errors"
	"fmt"
	"net/rpc"

	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
)

// GetShardID return mapping from key to shardID
func (c *Coordinator) GetShardID(key string) int64 {
	return common.SimpleHash(key, len(c.ShardToPeers))
}

// Leader returns rpc address if the cohort is leader, otherwise return empty string
func (c *Coordinator) Leader(address string) (string, error) {

	var response raftpb.RPCResponse
	cmd := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: common.LEADER,
			},
		},
	}

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return "", err
	}

	err = client.Call("Cohort.ProcessCommands", cmd, &response)
	return response.Addr, err
}

// FindLeader returns leader address in form (ip:port) and
// shard id.
// TODO: optimize FindLeader
func (c *Coordinator) FindLeader(key string) (string, int64, error) {

	shardID := c.GetShardID(key)
	// make rpc calls to get the leader
	nodes := c.ShardToPeers[shardID]

	for _, nodeAddr := range nodes {
		leader, err := c.Leader(nodeAddr)

		if err == nil && leader != "" {
			return nodeAddr, shardID, nil
		}
	}
	return "", -1, fmt.Errorf("shard %d is not reachable", shardID)
}

// SendMessageToShard sends prepare message to a shard. The return value
// indicates if the shard successfully performed the operation.
func (c *Coordinator) SendMessageToShard(ops *raftpb.ShardOps) ([]*raftpb.Command, error) {
	var response raftpb.RPCResponse
	// Figure out leader for the shard
	addr, _, err := c.FindLeader(ops.MasterKey)
	if err != nil {
		c.log.Error(err)
		return nil, err
	}
	// TODO: Add retries, time out handled by library.
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		c.log.Error(err)
		return nil, err
	}

	err = client.Call("Cohort.ProcessTransactionMessages", ops, &response)
	if err != nil {
		c.log.Error(err)
		return nil, err
	}

	success := response.Phase == (common.Prepared) || response.Phase == (common.Committed) || response.Phase == (common.Aborted)
	if success {
		return response.Commands, nil
	}
	return response.Commands, errors.New(response.Phase)
}

// RetryCommit ...
func (c *Coordinator) RetryCommit(txid string, gt *raftpb.GlobalTransaction) error {

	c.log.Infof("Recovering transaction: %s performing Commit", txid)
	var commitResponses int
	numShards := len(gt.ShardToCommands)
	for id, shardOps := range gt.ShardToCommands {
		c.log.Infof("[txid %s] Phase for shard: %s -> %s", txid, id, shardOps.Phase)
		if _, err := c.SendMessageToShard(shardOps); err == nil {
			commitResponses++
		} else {
			c.log.Errorf("Sending message shard failed:%s", err)
		}
	}

	if commitResponses != numShards {
		return errors.New("recovery of transaction failed, retrying in next cycle")
	}

	gt.Phase = common.Committed
	c.log.Infof("Setting txid:%s to :%s", txid, gt.Phase)
	if err := c.Replicate(txid, common.SET, gt); err != nil {
		return fmt.Errorf("[txid: %s] failed to set commited state: %s", txid, err)

	}

	c.log.Infof("[txid :%s] Commit Ack recieved: %d Ack Expected: %d", txid, commitResponses, numShards)
	c.log.Infof("transaction recovery successfully for: %s", txid)
	return nil
}

// RetryAbort ...
func (c *Coordinator) RetryAbort(txid string, gt *raftpb.GlobalTransaction) error {

	var err error
	var abortMessages int
	numShards := len(gt.ShardToCommands)
	c.log.Infof("Recovering transaction: %s performing Abort", txid)
	for _, shardops := range gt.ShardToCommands {
		shardops.Phase = common.Abort
		// best effort
		_, err = c.SendMessageToShard(shardops)
		if err != nil {
			return fmt.Errorf("[txid %s] failed at %v with %s", txid, shardops, err.Error())
		} else {
			abortMessages++
		}
	}

	if abortMessages != numShards {
		return errors.New("recovery of transaction - abort failed")
	}

	gt.Phase = common.Aborted
	// replicate via raft
	if err := c.Replicate(txid, common.SET, gt); err != nil {
		return fmt.Errorf("[txid: %s] failed to set Aborted state: %s", txid, err)
	}
	c.log.Infof("[txid: %s] Aborted Successfully during recovery", txid)
	return nil
}
