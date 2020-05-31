package coordinator

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/config"
	"github.com/raft-kv-store/raftpb"
	log "github.com/sirupsen/logrus"
)

// Coordinator ...
type Coordinator struct {
	ID          string
	RaftAddress string
	RaftDir     string

	Raft *raft.Raft // The consensus mechanism

	// coordinator state - This has to be replicated.
	// TODO: concurrent transactions
	txMap map[string]*raftpb.GlobalTransaction
	m     sync.Mutex

	// ShardToPeers need to be populated based on a config.
	// If time permits, these can be auto-discovered.
	ShardToPeers map[int64][]string

	Client *rpc.Client
	log    *log.Entry
}

// NewCoordinator initialises the new coordinator instance
func NewCoordinator(logger *log.Logger, nodeID, raftDir, raftAddress string, enableSingle bool) *Coordinator {

	if nodeID == "" {
		nodeID = "node-" + common.RandNodeID(common.NodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	log := logger.WithField("component", "coordinator")
	log.Infof("Preparing node-%s with persistent directory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)

	shardsInfo, err := config.GetShards()
	if err != nil {
		log.Fatal(err)
	}
	shardToPeers := make(map[int64][]string)
	for i, shard := range shardsInfo.Shards {
		shardToPeers[int64(i)] = append(shardToPeers[int64(i)], shard...)
	}

	c := &Coordinator{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RaftDir:     raftDir,

		ShardToPeers: shardToPeers,
		txMap:        make(map[string]*raftpb.GlobalTransaction),
		log:          log,
	}

	ra, err := common.SetupRaft((*fsm)(c), c.ID, c.RaftAddress, c.RaftDir, enableSingle)
	if err != nil {
		log.Fatalf("Unable to setup raft instance for kv store:%s", err)
	}

	c.Raft = ra

	return c
}

// Replicate replicates put/get/deletes on coordinator's
// state machine
func (c *Coordinator) Replicate(key, op string, gt *raftpb.GlobalTransaction) error {

	var cmd *raftpb.RaftCommand
	switch op {

	case common.SET:
		cmd = &raftpb.RaftCommand{
			Commands: []*raftpb.Command{
				{
					Method: op,
					Key:    key,
					Gt:     gt,
				},
			},
		}

	case common.DEL:
		cmd = &raftpb.RaftCommand{
			Commands: []*raftpb.Command{
				{
					Method: op,
					Key:    key,
				},
			},
		}
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	f := c.Raft.Apply(b, common.RaftTimeout)

	return f.Error()
}
