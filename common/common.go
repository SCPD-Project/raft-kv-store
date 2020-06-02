package common

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	GET      = "get"
	SET      = "set"
	DEL      = "del"
	LEADER   = "leader"
	EXIT     = "exit"
	TXN      = "txn"
	ADD      = "add"
	SUB      = "sub"
	ENDTXN   = "end"
	TRANSFER = "xfer"

	Prepare = "Prepare"
	Commit  = "Commit"

	Prepared  = "Prepared"
	Committed = "Committed"
	Aborted   = "Aborted"

	NotPrepared = "NotPrepared"
	Invalid     = "Invalid"
	Abort       = "Abort"

	RetainSnapshotCount = 2
	RaftTimeout         = 10 * time.Second
	NodeIDLen           = 5

	MagicDiff = 20000
)

var (
	SnapshotThreshold int
	SnapshotInterval  int
)

// RandNodeID returns a random node id
func RandNodeID(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// SetupRaft initialises raft and returns a raft instance. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func SetupRaft(fsm raft.FSM, id, raftAddress, raftDir string, enableSingle bool) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	// Override defaults with configured values
	config.SnapshotThreshold = uint64(SnapshotThreshold)
	config.SnapshotInterval = time.Duration(SnapshotInterval) * time.Second
	config.LocalID = raft.ServerID(id)
	config.LogLevel = "INFO"

	// Setup Raft communication.
	var TCPAddress *net.TCPAddr
	var transport *raft.NetworkTransport
	var err error
	var snapshots *raft.FileSnapshotStore
	if TCPAddress, err = net.ResolveTCPAddr("tcp", raftAddress); err != nil {
		log.Fatalf("failed to resolve TCP address %s: %s", raftAddress, err)
	}
	if transport, err = raft.NewTCPTransport(raftAddress, TCPAddress, 3, 10*time.Second, os.Stderr); err != nil {
		log.Fatalf("failed to make TCP transport on %s: %s", raftAddress, err.Error())
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	if snapshots, err = raft.NewFileSnapshotStore(raftDir, RetainSnapshotCount, os.Stderr); err != nil {
		log.Fatalf("failed to create snapshot store at %s: %s", raftDir, err.Error())
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create new raft: %s", err)
	}
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

	return ra, nil
}

// GetDerivedAddress derives a new IP:Port from a given
// address.
func GetDerivedAddress(address string) string {

	ipPort := strings.Split(address, ":")
	port, err := strconv.ParseInt(ipPort[1], 10, 32)
	if err != nil {
		log.Fatalf("Invalid raft port for store: %s", err)
	}
	return ipPort[0] + ":" + strconv.Itoa(int(port+MagicDiff))
}
