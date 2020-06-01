package store

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jinzhu/copier"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
)

type cohortfsm Cohort

// Apply applies a Raft log entry to the key-value store.
func (f *cohortfsm) Apply(l *raft.Log) interface{} {

	var raftCommand raftpb.RaftCommand
	if err := proto.Unmarshal(l.Data, &raftCommand); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	if len(raftCommand.Commands) > 1 {
		return errors.New("invalid command for cohort fsm")
	}

	command := raftCommand.Commands[0]

	switch command.Method {
	case common.SET:
		f.mu.Lock()
		defer f.mu.Unlock()
		f.opsMap[command.Key] = command.So

	case common.DEL:
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.opsMap, command.Key)
	default:
		panic(fmt.Sprintf("unrecognized command: %+v", command))
	}
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *cohortfsm) Snapshot() (raft.FSMSnapshot, error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	o := raftpb.OpsMap{
		Map: make(map[string]*raftpb.ShardOps),
	}
	for k, v := range f.opsMap {
		so := &raftpb.ShardOps{}
		copier.Copy(so, v)
		o.Map[k] = so
	}
	return &cohortfsmSnapshot{opsMap: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *cohortfsm) Restore(rc io.ReadCloser) error {

	// TODO: verify restore works as desired
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		f.store.log.Fatal(err)
	}

	var o *raftpb.OpsMap
	if err := proto.Unmarshal(b, o); err != nil {
		return err
	}

	f.opsMap = o.Map
	return nil
}

type cohortfsmSnapshot struct {
	opsMap raftpb.OpsMap
}

func (f *cohortfsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *cohortfsmSnapshot) Release() {}
