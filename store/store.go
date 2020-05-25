// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"

	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	"github.com/hashicorp/raft"
	log "github.com/sirupsen/logrus"
)

const (
	SnapshotPersistFile = "persistedKeyValues.db"
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	ID          string
	RaftDir     string
	RaftAddress string

	rpcAddress string

	mu sync.Mutex
	kv map[string]string // The key-value store for the system.

	transactionInProgress bool
	t                     sync.Mutex

	raft              *raft.Raft // The consensus mechanism
	log               *log.Entry
	persistBucketName string
	persistKvDbConn   *persistKvDB // persistent store

}

// NewStore returns a new Store.
func NewStore(logger *log.Logger, nodeID, raftAddress, raftDir string, enableSingle bool, rpcAddress string, bucketName string) *Store {
	if nodeID == "" {
		nodeID = "node-" + common.RandNodeID(common.NodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	l := logger.WithField("component", "store")
	l.Infof("Preparing node-%s with persistent directory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)
	if bucketName == "" {
		bucketName = "bucket-" + nodeID
	}
	persistDbConn := newDBConn(filepath.Join(raftDir, nodeID+"-"+SnapshotPersistFile), bucketName, logger)

	s := &Store{
		ID:                nodeID,
		RaftAddress:       raftAddress,
		RaftDir:           raftDir,
		kv:                make(map[string]string),
		log:               l,
		rpcAddress:        rpcAddress,
		persistKvDbConn:   persistDbConn,
		persistBucketName: bucketName,
	}

	ra, err := common.SetupRaft((*fsm)(s), s.ID, s.RaftAddress, s.RaftDir, enableSingle)
	if err != nil {
		l.Fatalf("Unable to setup raft instance for kv store:%s", err)
	}
	s.raft = ra
	go startCohort(s, rpcAddress)
	return s
}

// Start ...
func (s *Store) Start(joinHTTPAddress, id string) {

	// no op if you are leader
	if joinHTTPAddress == "" {
		return
	}
	var response common.RPCResponse
	msg := &raftpb.JoinMsg{RaftAddress: s.RaftAddress, ID: id}

	client, err := rpc.DialHTTP("tcp", joinHTTPAddress)
	if err != nil {
		s.log.Fatalf("Unable to reach leader: %s", err)
	}

	err = client.Call("Cohort.ProcessJoin", msg, &response)
	if err != nil {
		s.log.Fatalf("Unable to join cluster: %s", err)

	}
}

// Leader returns the current leader of the cluster
func (s *Store) Leader() string {
	return string(s.raft.Leader() + "\n")
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.log.Infof("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.log.Infof("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.log.Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.log.Infof("node %s at %s joined successfully", nodeID, addr)
	return nil
}
