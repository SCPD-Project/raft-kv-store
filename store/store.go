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
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	nodeIDLen           = 5
	SnapshotPersistFile = "persistedKeyValues.db"
)

var (
	SnapshotThreshold int
	SnapshotInterval int
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	ID          string
	RaftDir     string
	RaftAddress string

	mu sync.Mutex
	kv map[string]string // The key-value store for the system.

	transactionInProgress bool
	t                     sync.Mutex

	raft   *raft.Raft // The consensus mechanism
	log	   *log.Entry
	persistBucketName string
	persistKvDbConn   *persistKvDB // persistent store

}

// NewStore returns a new Store.
func NewStore(logger *log.Logger, nodeID, raftAddress, raftDir string,
	          bucketName string) *Store {
	if nodeID == "" {
		nodeID = "node-" + randNodeID(nodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	l := logger.WithField("component", "store")
	l.Infof("Preparing node-%s with persistent diretory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)
	if bucketName == "" {
		bucketName = "bucket-" + nodeID
	}
	persistDbConn, err := newDBConn(filepath.Join(raftDir, nodeID + SnapshotPersistFile), bucketName)
	if err != nil {
		l.Fatalf(" Failed creating persistent store with err: %s", err)
	}

	s := &Store{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RaftDir:     raftDir,
		kv:          make(map[string]string),
		log:      l,
		persistKvDbConn: persistDbConn,
		persistBucketName: bucketName,
	}
	return s
}

func randNodeID(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	// Override defaults with configured values
	config.SnapshotThreshold = uint64(SnapshotThreshold)
	config.SnapshotInterval = time.Duration(SnapshotInterval) * time.Second
	config.LocalID = raft.ServerID(s.ID)

	// Setup Raft communication.
	var TCPAddress *net.TCPAddr
	var transport *raft.NetworkTransport
	var err error
	var snapshots *raft.FileSnapshotStore
	if TCPAddress, err = net.ResolveTCPAddr("tcp", s.RaftAddress); err != nil {
		s.log.Fatalf("failed to resolve TCP address %s: %s", s.RaftAddress, err)
	}
	if transport, err = raft.NewTCPTransport(s.RaftAddress, TCPAddress, 3, 10*time.Second, os.Stderr); err != nil {
		s.log.Fatalf("failed to make TCP transport on %s: %s", s.RaftAddress, err.Error())
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	if snapshots, err = raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr); err != nil {
		s.log.Fatalf("failed to create snapshot store at %s: %s", s.RaftDir, err.Error())
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		s.log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		s.log.Fatalf("failed to create new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log.Infof("Processing Get request %s", key)
	val, ok := s.kv[key]
	if !ok {
		return "", fmt.Errorf("Key=%s does not exist", key)
	}

	return val, nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	s.log.Infof("Processing Set request: Key=%s Value=%s", key, value)
	c := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: raftpb.SET,
				Key:    key,
				Value:  value,
			},
		},
	}

	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	log.Printf("Processing Delete request: Key=%s", key)
	if _, err := s.Get(key); err != nil {
		return err
	}
	c := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: raftpb.DEL,
				Key:    key,
			},
		},
	}

	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Transaction atomically executes the transaction .
func (s *Store) Transaction(ops []*raftpb.Command) error {
	if s.raft.State() != raft.Leader {
		s.log.Error(raft.ErrNotLeader)
		return raft.ErrNotLeader
	}

	if s.transactionInProgress {
		return fmt.Errorf("transaction in progress, try again")
	}

	s.t.Lock()
	s.transactionInProgress = true
	s.t.Unlock()

	c := &raftpb.RaftCommand{
		Commands: ops,
	}

	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	s.t.Lock()
	s.transactionInProgress = false
	s.t.Unlock()

	return f.Error()
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
