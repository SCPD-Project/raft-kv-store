package common

import (
	"github.com/raft-kv-store/raftpb"
)

// Phase is enum for different phases of
// 2 PC
type Phase string

const (
	// Prepare ...
	Prepare Phase = "prepare"
	Commit  Phase = "commit"

	Prepared  Phase = "prepared"
	Committed Phase = "committed"
	Aborted   Phase = "Aborted"

	NotPrepared Phase = "NotPrepared"
	Invalid     Phase = "Invalid"
	Abort       Phase = "Abort"
)

// GlobalTransaction captures the info of entire transaction
type GlobalTransaction struct {
	Txid string
	Cmds *raftpb.RaftCommand
	// Cohorts consists of leader's of all the shards.
	// TODO: It can be
	// used for transaction stats etc in the future.
	Cohorts         []string
	ShardToCommands map[int]*ShardOps
	Phase           Phase
}

// ShardOps captures cmds that need to be executed
// by a shard. MasterKey refers to the key of the first
// operation. It is used to route the request to the leader
// of the shard.
type ShardOps struct {
	Txid      string
	MasterKey string
	Cmds      *raftpb.RaftCommand
	// Phase indicates the type of message (Prepare/Commit/Abort).
	Phase Phase
}

// RPCResponse is the response from rpc call.
// TODO: Add enum for status
// Separate out value into different type: required???
type RPCResponse struct {
	Status int
	Value  string
}
