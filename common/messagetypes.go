package common
//
//const (
//	// Prepare ...
//	Prepare  = "prepare"
//	Commit   = "commit"
//
//	Prepared   = "prepared"
//	Committed  = "committed"
//	Aborted    = "Aborted"
//
//	NotPrepared  = "NotPrepared"
//	Invalid      = "Invalid"
//	Abort        = "Abort"
//)

//// GlobalTransaction captures the info of entire transaction
//type GlobalTransaction struct {
//	Txid string
//	Cmds *raftpb.RaftCommand
//	// Cohorts consists of leader's of all the shards.
//	// TODO: It can be
//	// used for transaction stats etc in the future.
//	Cohorts         []string
//	ShardToCommands map[int]*ShardOps
//	Phase           string
//}

// ShardOps captures cmds that need to be executed
// by a shard. MasterKey refers to the key of the first
// operation. It is used to route the request to the leader
// of the shard.
//type ShardOps struct {
//	Txid      string
//	MasterKey string
//	Cmds      *raftpb.RaftCommand
//	// Phase indicates the type of message (Prepare/Commit/Abort).
//	Phase string
//}

// RPCResponse is the response from rpc call.
// TODO: Add enum for status
// Separate out value into different type: required???
//type RPCResponse struct {
//	Status int
//	Value  string
//}
