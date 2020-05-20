package coordinator

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/RAFT-KV-STORE/common"

	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jinzhu/copier"
)

type fsm Coordinator

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {

	var raftCommand raftpb.RaftCommand
	if err := proto.Unmarshal(l.Data, &raftCommand); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	if len(raftCommand.Commands) > 1 {
		return fmt.Errorf("invalid command for coordnator fsm")
	}

	command := raftCommand.Commands[0]

	switch command.Method {
	case common.SET:
		f.m.Lock()
		defer f.m.Unlock()

		var gt common.GlobalTransaction
		err := json.Unmarshal([]byte(command.Value), &gt)
		if err != nil {
			return fmt.Errorf("log entry corrupted")
		}
		f.cstate[command.Key] = &gt

	case common.DELETE:
		f.m.Lock()
		defer f.m.Unlock()

		delete(f.cstate, command.Key)
	default:
		panic(fmt.Sprintf("unrecognized command: %+v", command))
	}
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {

	f.m.Lock()
	defer f.m.Unlock()

	o := make(map[string]*common.GlobalTransaction)
	for k, v := range f.cstate {
		gt := &common.GlobalTransaction{}
		copier.Copy(gt, v)
		o[k] = gt
	}
	return &fsmSnapshot{cstate: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {

	// TODO: verify restore works as desired
	o := make(map[string]*common.GlobalTransaction)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	f.cstate = o
	return nil
}

type fsmSnapshot struct {
	cstate map[string]*common.GlobalTransaction
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmSnapshot) Release() {}
