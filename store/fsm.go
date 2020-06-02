package store

import (
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	log "github.com/sirupsen/logrus"
)

type fsm Store

type FSMApplyResponse struct {
	reply raftpb.RPCResponse
	err   error
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var raftCommand raftpb.RaftCommand
	if err := proto.Unmarshal(l.Data, &raftCommand); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}
	f.log.Infof("Apply %v", raftCommand)
	// txn set is locked already in prepare
	if !raftCommand.IsTxn {
		command := raftCommand.Commands[0]
		switch command.Method {
		case common.SET:
			if command.Cond == nil {
				return f.applySet(command.Key, command.Value)
			} else {
				return f.applySetCond(command.Key, command.Value, command.Cond.Value)
			}
		case common.DEL:
			return f.applyDelete(command.Key)
		default:
			panic(fmt.Sprintf("unrecognized command: %+v", command))
		}
	}
	return f.applyTransaction(raftCommand.Commands)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	m := f.kv.Snapshot()

	// Clone the map.
	o := make(map[string]int64)
	for k, v := range m {
		o[k] = v.(int64)
	}

	return &fsmSnapshot{store: o, persistDBConn: f.persistKvDbConn, bucketName: f.persistBucketName,
		logger: f.log}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(_ io.ReadCloser) error {
	rst := make(map[string]int64)
	rst = f.restore()
	f.log.Infof(" Snapshot restore from bucket: %s with kv-size: %d", f.persistBucketName, len(rst))

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	o := make(map[string]interface{})
	for k, v := range rst {
		o[k] = v
	}
	f.kv = common.NewCmapFromMap(f.log.Logger, o, LockContention)
	return nil
}

func (f *fsm) applySet(key string, value int64) interface{} {

	err := f.kv.Set(key, value)
	if err == nil {
		return &FSMApplyResponse{
			reply: raftpb.RPCResponse{Status: 0},
		}
	}
	f.log.Infof(err.Error())
	return &FSMApplyResponse{
		err:   err,
		reply: raftpb.RPCResponse{Status: -1},
	}

}

func (f *fsm) applySetCond(key string, value, value0 int64) interface{} {

	err := f.kv.SetCond(key, value, value0)
	if err == nil {
		return &FSMApplyResponse{
			reply: raftpb.RPCResponse{Status: 0},
		}
	}

	return &FSMApplyResponse{
		err:   err,
		reply: raftpb.RPCResponse{Status: -1},
	}

}

func (f *fsm) applyDelete(key string) interface{} {
	err := f.kv.Del(key)
	if err == nil {
		return &FSMApplyResponse{
			reply: raftpb.RPCResponse{Status: 0},
		}
	}
	return &FSMApplyResponse{
		err:   err,
		reply: raftpb.RPCResponse{Status: -1},
	}

}

// return transaction result
func (f *fsm) applyTransaction(ops []*raftpb.Command) interface{} {
	// WriteWithLocks will fail in recovery
	// Write is safe as long as it's only used in txn
	f.kv.Write(ops)
	return nil
}

type fsmSnapshot struct {
	store         map[string]int64
	persistDBConn *persistKvDB
	bucketName    string
	logger        *log.Entry
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	f.logger.Infof(" Snapshot persisted to bucket: %s", f.bucketName)
	err := func() error {
		// Persist data.
		f.save()

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
