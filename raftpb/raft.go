package raftpb

import "github.com/raft-kv-store/common"

func (r *RaftCommand) isReadOnly() bool {
	for _, cmd := range r.Commands {
		if cmd.Method != common.GET {
			return false
		}
	}
	return true
}
