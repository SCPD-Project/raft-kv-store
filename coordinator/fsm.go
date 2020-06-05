package coordinator

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/jinzhu/copier"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
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
		if f.txMap != nil {
			f.txMap[command.Key] = command.Gt
		}

	case common.DEL:
		f.m.Lock()
		defer f.m.Unlock()
		delete(f.txMap, command.Key)
	default:
		panic(fmt.Sprintf("unrecognized command: %+v", command))
	}
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {

	f.m.Lock()
	defer f.m.Unlock()

	o := raftpb.TxidMap{
		Map: make(map[string]*raftpb.GlobalTransaction),
	}
	for k, v := range f.txMap {
		gt := &raftpb.GlobalTransaction{}
		copier.Copy(gt, v)
		o.Map[k] = gt
	}
	return &fsmSnapshot{txidMap: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {

	// TODO: verify restore works as desired
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		f.log.Fatal(err)
	}

	var o *raftpb.TxidMap
	//o := &raftpb.TxidMap{}
	if err := proto.Unmarshal(b, o); err != nil {
		return err
	}

	f.txMap = o.Map
	return nil
}

type fsmSnapshot struct {
	txidMap raftpb.TxidMap
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *fsmSnapshot) Release() {}
