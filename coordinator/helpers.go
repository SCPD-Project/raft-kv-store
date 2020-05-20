package coordinator

import (
	"fmt"
	"net/rpc"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/raftpb"
)

// GetShardID return mapping from key to shardid
func (c *Coordinator) GetShardID(key string) int {

	h := 0
	nshards := len(c.ShardToPeers)
	for _, c := range key {
		h = 31*h + int(c)
	}
	return h % nshards
}

// Leader returns leader of a shard. This call is made
// to any of the nodes in the shard to fetch latest
// leader.
func (c *Coordinator) Leader(address string) (string, error) {

	var response common.RPCResponse
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
	return response.Value, err
}

// FindLeader returns leader address in form (ip:port) and
// shard id.
func (c *Coordinator) FindLeader(key string) (string, int, error) {

	shardid := c.GetShardID(key)
	// make rpc calls to get the leader
	nodes := c.ShardToPeers[shardid]

	for _, nodeaddr := range nodes {
		leader, err := c.Leader(nodeaddr)

		if err == nil && leader != "" {
			return leader, shardid, nil
		}
	}
	return "", -1, fmt.Errorf("shard %d is not reachable", shardid)
}

// SendMessageToShard sends prepare message to a shard. The return value
// indicates if the shard successfully performed the operation. This returns bool
// as the caller need not care of the exact error
func (c *Coordinator) SendMessageToShard(ops *common.ShardOps) bool {

	var response common.RPCResponse

	// Figure out leader for the shard
	addr, _, err := c.FindLeader(ops.MasterKey)
	if err != nil {
		return false
	}

	// TODO: Add retries, time out handled by library.
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}

	err = client.Call("Cohort.ProcessTransactionMessages", ops, &response)
	if err != nil {
		return false
	}
	return response.Value == common.Prepared || response.Value == common.Committed || response.Value == common.Aborted
}
