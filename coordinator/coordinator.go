package coordinator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"

	"github.com/RAFT-KV-STORE/common"
	"github.com/RAFT-KV-STORE/config"
	"github.com/RAFT-KV-STORE/raftpb"
	"google.golang.org/protobuf/proto"

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
	m      sync.Mutex

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

// Replicate replicates put/get/deletes on coordinator's
// state machine
func (c *Coordinator) Replicate(key, op string, value *common.GlobalTransaction) error {

	var cmd *raftpb.RaftCommand
	gt, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("unable to marshal global transaction obj: %s", err)
	}

	switch op {

	case common.SET:
		cmd = &raftpb.RaftCommand{
			Commands: []*raftpb.Command{
				{
					Method: common.SET,
					Key:    key,
					Value:  string(gt),
				},
			},
		}

	case common.DELETE:
		cmd = &raftpb.RaftCommand{
			Commands: []*raftpb.Command{
				{
					Method: common.DELETE,
					Key:    key,
				},
			},
		}
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	f := c.raft.Apply(b, common.RaftTimeout)

	return f.Error()
}
