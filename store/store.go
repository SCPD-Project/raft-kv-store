// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"io"
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

// Ops indicate transaction ops
type Ops struct {
	Command string `json:"Command"`
	Key     string `json:"Key"`
	Value   string `json:"Value"`
}

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	nodeIDLen = 5
)

type RaftCommand struct {
	Ops []Ops `json:"ops,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	ID          string
	RaftDir     string
	RaftAddress string

	mu sync.Mutex
	kv map[string]string // The key-value store for the system.

	transactionInProgress bool
	t                     sync.Mutex

	raft *raft.Raft // The consensus mechanism
	logger *log.Logger
	file *os.File // persistent store
}

// NewStore returns a new Store.
func NewStore(nodeID, raftAddress, raftDir string) *Store {
	if nodeID == "" {
		nodeID = "node-" + randNodeID(nodeIDLen)
	}
	if raftDir == "" {
		raftDir = fmt.Sprintf("./%s", nodeID)
	}
	log.Printf("Preparing node-%s with persistent diretory %s, raftAddress %s", nodeID, raftDir, raftAddress)
	os.MkdirAll(raftDir, 0700)
	s := &Store{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RaftDir:     raftDir,
		kv:          make(map[string]string),
		logger:      log.New(os.Stderr, "[store] ", log.LstdFlags),
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
	var TCPaddress *net.TCPAddr
	var transport *raft.NetworkTransport
	var err error
	var snapshots *raft.FileSnapshotStore
	if TCPaddress, err = net.ResolveTCPAddr("tcp", s.RaftAddress); err != nil {
		log.Fatalf("failed to resolve TCP address %s: %s", s.RaftAddress, err)
	}
	if transport, err = raft.NewTCPTransport(s.RaftAddress, TCPaddress, 3, 10*time.Second, os.Stderr); err != nil {
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
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Print("Processing Get request", key)
	val, ok := s.kv[key]
	if !ok {
		return "", fmt.Errorf("Key does not exist")
	}

	return val, nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	log.Printf("Processing Set request :%s :%s", key, value)
	c := &RaftCommand{
		Ops: []Ops{
			{
				Command: "set",
				Key:     key,
				Value:   value,
			},
		},
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &RaftCommand{
		Ops: []Ops{
			{
				Command: "delete",
				Key:     key,
			},
		},
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Transaction atomically executes the transaction .
func (s *Store) Transaction(ops []Ops) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	if s.transactionInProgress {
		return fmt.Errorf("Transaction in progress, try again")
	}

	s.t.Lock()
	s.transactionInProgress = true
	s.t.Unlock()

	c := &RaftCommand{
		Ops: ops,
	}

	b, err := json.Marshal(c)
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

	return string(s.raft.Leader())
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

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {

	var raftCommand RaftCommand
	if err := json.Unmarshal(l.Data, &raftCommand); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	if len(raftCommand.Ops) == 1 {

		c := raftCommand.Ops[0]
		switch c.Command {
		case "set":

			return f.applySet(c.Key, c.Value)
		case "delete":
			return f.applyDelete(c.Key)

		default:
			panic(fmt.Sprintf("unrecognized command op: %s", c.Command))
		}
	}

	return f.applyTransaction(raftCommand.Ops)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.kv {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.kv = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.kv, key)
	return nil
}

// return transaction result
func (f *fsm) applyTransaction(ops []Ops) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, c := range ops {

		switch c.Command {
		case "set":
			f.kv[c.Key] = c.Value
		case "delete":
			delete(f.kv, c.Key)
		}
	}
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
