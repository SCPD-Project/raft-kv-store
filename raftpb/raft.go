package raftpb

func (r *RaftCommand) isReadOnly() bool {
	for _, cmd := range r.Commands {
		// TODO: temporary way to resolve circular imports
		// Need to move const to separate package.
		if cmd.Method != "get" {
			return false
		}
	}
	return true
}
