package common

import (
	"github.com/RAFT-KV-STORE/raftpb"
)

const (
	Prepare = "prepare"
	Commit  = "commit"

	Prepared  = "prepared"
	Committed = "committed"

	NotPrepared = "NotPrepared"
	Invalid     = "Invalid"
	Abort       = "Abort"
)

const (
	SET    = "set"
	DELETE = "delete"
	GET    = "get"
	LEADER = "leader"
)

// Message indicates the message format that is exchanged between
// coordinator and the cohorts
type Message struct {
	Txn  *LocalTransaction
	Type string
}

// LocalTransaction captures the content of the transaction. The ID (global id) and Cmds.
// The cmds will contain requests related to the shard. It is the responsibiliy of the
// coordinator to populate it correctly.
type LocalTransaction struct {
	Txid  int
	Cmds  raftpb.RaftCommand
	Phase string
}

// GlobalTransaction captures the info of entire transaction
type GlobalTransaction struct {
	Txid string
	Cmds *raftpb.RaftCommand
	// Cohorts consists of leader's of all the shards. It can be
	// used for transaction stats etc in the future.
	Cohorts         []string
	ShardToCommands map[int]*ShardOps
	Phase           string
}

// ShardOps captures cmds that need to be executed
// by a shard. MasterKey refers to the key of the first
// operation. It is used to route the request to the leader
// of the shard.
type ShardOps struct {
	Txid      string
	MasterKey string
	Cmds      *raftpb.RaftCommand
	// MessageType indicates the type of message (Prepare/Commit/Abort).
	MessageType string
}

// RPCResponse is the response from rpc call. The value is mostly
// used for Get cmds and fetching leader from other cluster.
// Other commands do not return any value apart
// from success. We could use an interface type as well, but this is
// just easier.
type RPCResponse struct {
	Status int
	Value  string
}
