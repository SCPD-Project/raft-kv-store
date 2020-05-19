package coordinator

import (
	"fmt"
	"log"
	"net/rpc"
	"os"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/config"

	"github.com/hashicorp/raft"
)

// Coordinator ...
type Coordinator struct {
	ID          string
	RaftAddress string
	RaftDir     string

	raft *raft.Raft // The consensus mechanism

	// coordinator state - This has to be replicated.
	cstate map[string]*common.GlobalTransaction

	// ShardToPeers need to be populated based on a config.
	// If time permits, these can be auto-discovered.
	ShardToPeers map[int][]string

	Client *rpc.Client
}

// New initailises the new co-ordinator instance
func New(nodeID, raftDir, raftAddress string, enableSingle bool, shardInfo *config.Config) (*Coordinator, error) {

	if nodeID == "" {
		nodeID = "node-" + common.RandNodeID(common.NodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	log.Printf("Preparing node-%s with persistent directory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)

	shardToPeers := make(map[int][]string)
	for i, shard := range shardInfo.Shards {
		shardToPeers[i] = append(shardToPeers[i], shard...)
	}

	c := &Coordinator{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RaftDir:     raftDir,

		ShardToPeers: shardToPeers,
		cstate:       make(map[string]*common.GlobalTransaction),
	}

	ra, err := common.SetupRaft((*fsm)(c), c.ID, c.RaftAddress, c.RaftDir, enableSingle)
	if err != nil {
		return nil, fmt.Errorf("Unable to setup raft instance for kv store:%s", err)
	}

	c.raft = ra

	return c, nil
}
