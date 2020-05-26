package store

import (
	"fmt"
	"github.com/RAFT-KV-STORE/common"
	"io"

	"github.com/raft-kv-store/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	log "github.com/sirupsen/logrus"
)

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var raftCommand raftpb.RaftCommand
	if err := proto.Unmarshal(l.Data, &raftCommand); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}
	if len(raftCommand.Commands) == 1 {
		command := raftCommand.Commands[0]
		switch command.Method {
		case common.SET:
			return f.applySet(command.Key, command.Value)
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
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]int64)
	for k, v := range f.kv {
		o[k] = v
	}

	return &fsmSnapshot{store: o, persistDBConn: f.persistKvDbConn, bucketName: f.persistBucketName,
		logger: f.log}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(_ io.ReadCloser) error {
	o := make(map[string]int64)
	o = f.restore()
	f.log.Infof(" Snapshot restore from bucket: %s with kv-size: %d", f.persistBucketName, len(o))

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.kv = o
	return nil
}

func (f *fsm) applySet(key string, value int64) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.kv, key)
	return nil
}

// return transaction result
func (f *fsm) applyTransaction(ops []*raftpb.Command) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, command := range ops {
		switch command.Method {
		case common.SET:
			f.kv[command.Key] = command.Value
		case common.DEL:
			delete(f.kv, command.Key)
		}
	}
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
