// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	//"encoding/json"
	"fmt"
	"github.com/SCPD-Project/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
	//"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	nodeIDLen           = 5

	SET = "set"
	DELETE = "delete"
)

//type RaftCommand struct {
//	Ops []Ops `json:"ops,omitempty"`
//}

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
	logger *log.Logger
	dbserver *server
}

// NewStore returns a new Store.
func NewStore(nodeID, raftAddress, raftDir string, file string) *Store {
	if nodeID == "" {
		nodeID = "node-" + randNodeID(nodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	log.Printf("Preparing node-%s with persistent diretory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)
	boltDb, err := createNewServer(filepath.Join(raftDir, file))
	if err != nil {
		log.Fatalf("failed to create new bolt persistent store: %s", err)
	}
	s := &Store{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RaftDir:     raftDir,
		kv:          make(map[string]string),
		logger:      log.New(os.Stderr, "[store] ", log.LstdFlags),
		dbserver:    boltDb,
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
	config.LocalID = raft.ServerID(s.ID)

	// Setup Raft communication.
	var TCPAddress *net.TCPAddr
	var transport *raft.NetworkTransport
	var err error
	var snapshots *raft.FileSnapshotStore
	if TCPAddress, err = net.ResolveTCPAddr("tcp", s.RaftAddress); err != nil {
		log.Fatalf("failed to resolve TCP address %s: %s", s.RaftAddress, err)
	}
	if transport, err = raft.NewTCPTransport(s.RaftAddress, TCPAddress, 3, 10*time.Second, os.Stderr); err != nil {
		log.Fatalf("failed to make TCP transport on %s: %s", s.RaftAddress, err.Error())
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	if snapshots, err = raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr); err != nil {
		log.Fatalf("failed to create snapshot store at %s: %s", s.RaftDir, err.Error())
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Fatalf("failed to create new raft: %s", err)
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

	log.Printf("Processing Get request %s", key)
	val, err := s.dbserver.PersistGet(key)
	if err != nil {
		log.Printf("Failure in processing key %s %s", key, err)
	}

	return val, nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	log.Printf("Processing Set request: Key=%s Value=%s", key, value)
	c := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: SET,
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
	s.dbserver.PersistPut([]byte(key), []byte(value))
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	c := &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{
				Method: DELETE,
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
		log.Print("not leader")
		return fmt.Errorf("not leader")
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
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
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
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}
