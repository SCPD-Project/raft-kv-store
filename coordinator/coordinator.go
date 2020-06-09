package coordinator

import (
	"errors"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/config"
	"github.com/raft-kv-store/raftpb"
	log "github.com/sirupsen/logrus"
)

const (
	// TransactionTimeout ...
	TransactionTimeout = 5
	// RecoveryInterval ...
	RecoveryInterval = 60
)

const (
	// FailPrepared indicates the transaction to fail after the prepare phase
	FailPrepared = "prepared"

	// FailCommit indicates the transaction to fail after the commit phase
	FailCommit = "commit"
)

// Coordinator ...
type Coordinator struct {
	ID          string
	RaftAddress string
	RaftDir     string

	raft *raft.Raft // The consensus mechanism

	// coordinator state - This has to be replicated.
	// TODO: concurrent transactions
	txMap map[string]*raftpb.GlobalTransaction
	mu    sync.RWMutex

	// ShardToPeers need to be populated based on a config.
	// If time permits, these can be auto-discovered.
	ShardToPeers map[int64][]string

	Client   *rpc.Client
	log      *log.Entry
	failmode string
}

// NewCoordinator initialises the new coordinator instance
func NewCoordinator(logger *log.Logger, nodeID, raftDir, raftAddress string, enableSingle bool, failmode string) *Coordinator {
	if nodeID == "" {
		nodeID = "node-" + common.RandNodeID(common.NodeIDLen)
	}

	if raftDir == "" {
		raftDir, _ = os.Hostname()
	}

	log := logger.WithField("component", "coordinator")
	coordDir := filepath.Join(common.RaftPVBaseDir, raftDir, "coords")
	log.Infof("Preparing node-%s with persistent directory %s, raftAddress %s", nodeID, coordDir, raftAddress)
	os.MkdirAll(coordDir, 0700)

	shardsInfo, err := config.GetShards()
	if err != nil {
		log.Fatal(err)
	}

	shardToPeers := make(map[int64][]string)
	for i, shard := range shardsInfo.Shards {
		shardToPeers[int64(i)] = append(shardToPeers[int64(i)], shard...)
	}

	c := &Coordinator{
		ID:           nodeID,
		RaftAddress:  raftAddress,
		RaftDir:      coordDir,
		ShardToPeers: shardToPeers,
		txMap:        make(map[string]*raftpb.GlobalTransaction),
		log:          log,
		failmode:     failmode,
	}

	ra, err := common.SetupRaft((*fsm)(c), c.ID, c.RaftAddress, c.RaftDir, enableSingle)
	if err != nil {
		log.Fatalf("Unable to setup raft instance for kv store:%s", err)
	}

	c.raft = ra

	go c.periodicRecovery()
	log.Info("Starting coordniator")
	return c
}

// periodicRecovery Runs periodically or on raft status changes.
func (c *Coordinator) periodicRecovery() {

	for {

		select {
		case <-c.raft.LeaderCh():
			c.log.Infof("Leader Change")
		case <-time.After(time.Duration(RecoveryInterval) * time.Second):
		}

		// Do this safety check
		if c.IsLeader() && len(c.txMap) > 0 {
			c.log.Infof("Starting Transaction Recovery")
			c.recoverTransactions()
		}

	}
}

// recoverTransactions traverses the entire txid map and
// takes below actions. This is similar to garbage collection aka
// stop the world
func (c *Coordinator) recoverTransactions() {

	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	for txid, gt := range c.txMap {
		// 3 phases
		// if transaction is not complete in 3 minutes
		if time.Since(time.Unix(0, gt.StartTime)).Seconds() >= TransactionTimeout {

			c.log.Infof("[txid: %s] is in phase :%s", txid, gt.GetPhase())
			switch gt.GetPhase() {

			case common.Prepare, common.Abort:
				err = c.RetryAbort(txid, gt)
				if err != nil {
					c.log.Errorf("[txid: %s] recovery unsuccessfull :%s", txid, err)
				}

			case common.Prepared, common.Commit:
				err = c.RetryCommit(txid, gt)
				if err != nil {
					c.log.Errorf("[txid: %s] recovery unsuccessfull :%s", txid, err)
				}

			case common.Aborted, common.Committed:
				delete(c.txMap, txid)

			}

		}
	}
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

	f := c.raft.Apply(b, common.RaftTimeout)

	return f.Error()
}

// IsLeader return if coordinator is leader of cluster.
func (c *Coordinator) IsLeader() bool {

	return c.raft.State() == raft.Leader
}

func (c *Coordinator) FindClusterLeader() (string, error) {
	// (If time permits): Make RPC calls to modify default leader behavior to fetch the host name of the leader
	leader := string(c.raft.Leader())
	if leader == "" {
		c.log.Info("Raft coordinator leader unavailable for now")
		return "", errors.New("no leader available")
	}

	return leader, nil
}
