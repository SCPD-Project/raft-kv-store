package coordinator

import (
	"fmt"
	"net/rpc"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/raftpb"
)

// GetShardID return mapping from key to shardID
func (c *Coordinator) GetShardID(key string) int64 {

	h := 0
	nshards := len(c.ShardToPeers)
	for _, c := range key {
		h = 31*h + int(c)
	}
	return int64(h % nshards)
}

// Leader returns leader of a shard. This call is made
// to any of the nodes in the shard to fetch latest
// leader.
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
			return leader, shardID, nil
		}
	}
	return "", -1, fmt.Errorf("shard %d is not reachable", shardID)
}

// SendMessageToShard sends prepare message to a shard. The return value
// indicates if the shard successfully performed the operation. This returns bool
// as the caller need not care of the exact error
func (c *Coordinator) SendMessageToShard(ops *raftpb.ShardOps) bool {

	var response raftpb.RPCResponse

	// Figure out leader for the shard
	addr, _, err := c.FindLeader(ops.MasterKey)
	if err != nil {
		return false
	}

	// TODO: Add retries, time out handled by library.
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		c.log.Error(err)
		return false
	}

	err = client.Call("Cohort.ProcessTransactionMessages", ops, &response)
	if err != nil {
		c.log.Error(err)
		return false
	}
	return response.Phase == (common.Prepared) || response.Phase == (common.Committed) || response.Phase == (common.Aborted)

}
