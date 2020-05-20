package common

import (
	"github.com/RAFT-KV-STORE/raftpb"
)

const (
	Prepare = "prepare"
	Commit  = "commit"

	Prepared  = "prepared"
	Committed = "committed"
	Aborted   = "Aborted"

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

// RPCResponse is the response from rpc call.
type RPCResponse struct {
	Status int
	Value  string
}
