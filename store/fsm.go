package store

import (
	"encoding/json"
	"fmt"
	"github.com/SCPD-Project/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"io"
	"log"
	//"github.com/boltdb/bolt"
	//"time"
	//"encoding/gob"
	//"bytes"
	//"io/ioutil"
	"github.com/boltdb/bolt"
	"time"
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
		case SET:
			return f.applySet(command.Key, command.Value)
		case DELETE:
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
	o := make(map[string]string)
	for k, v := range f.kv {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.kv = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = value
	go f.WriteToPersistence(key, value)
	return nil
}

func (f *fsm) WriteToPersistence(key string, value string) {
	//b , _ := json.Marshal(f.kv)
	/*var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.
	_ = enc.Encode(f.kv)*/
	db, err := bolt.Open("persist.db", 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("TestBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("TestBucket"))
		err := b.Put([]byte(key), []byte(value))
		return err
	})
	//ioutil.WriteFile("log.gob", network.Bytes(), 0600)
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
		case SET:
			f.kv[command.Key] = command.Value
		case DELETE:
			delete(f.kv, command.Key)
		}
	}
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
