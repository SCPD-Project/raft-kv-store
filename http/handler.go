package http

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/raft-kv-store/raftpb"
	"github.com/golang/protobuf/proto"
)

type SetJSON map[string]string
type TxnCommand struct {
	Command string `json:"command"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}
type TxnJSON struct {
	Commands []TxnCommand
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {

	msg, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.log.Fatal("Error when reading join data", err)
	}

	var joinMsg raftpb.JoinMsg
	if err = proto.Unmarshal(msg, &joinMsg); err != nil {
		s.log.Fatal("Error when unmarshal join data", err)
	}

	if joinMsg.RaftAddress == "" || joinMsg.ID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.coordinator.Join(joinMsg.ID, joinMsg.RaftAddress); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {

	getKey := func(path string) string {
		parts := strings.SplitN(path, "/", 3)
		if len(parts) != 3 {
			s.log.Fatalf("Error in getting key from %s", r.URL.Path)
		}
		return parts[2]
	}
	var msg string
	switch r.Method {
	case http.MethodGet:
		key := getKey(r.URL.Path)
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		val, err := s.coordinator.Get(key)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg = err.Error()
		} else {
			w.WriteHeader(http.StatusOK)
			msg = fmt.Sprintf("Key=%s, Value=%s", key, val)
		}

		io.WriteString(w, msg)

	case http.MethodPost:
		var m SetJSON
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg = fmt.Sprintf("failed to parse %v", r.Body)
		} else {
			if len(m) != 1 {
				w.WriteHeader(http.StatusInternalServerError)
				msg = fmt.Sprintf("Unable to set: %s has len > 1", m)
			} else {
				var k, v string
				for k, v = range m {
				}
				if err := s.coordinator.Set(k, v); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					msg = fmt.Sprintf("Unable to set: %s", err.Error())
				} else {
					w.WriteHeader(http.StatusOK)
				}
			}
		}
		io.WriteString(w, msg)

	case http.MethodDelete:
		key := getKey(r.URL.Path)
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			msg = "key is missing"
		} else if err := s.coordinator.Delete(key); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg = err.Error()
		} else {
			w.WriteHeader(http.StatusOK)
		}
		io.WriteString(w, msg)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if msg != "" {
		s.log.Info(msg)
	}
	return
}

// TODO: No raft leader api exposed in coordinator
// // handleLeader mainly used for debugs.
// func (s *Service) handleLeader(w http.ResponseWriter, r *http.Request) {

// 	s.log.Debug("Handling request for leader")
// 	io.WriteString(w, string(s.coordinator.Leader()))

// }

// handleTransaction
func (s *Service) handleTransaction(w http.ResponseWriter, r *http.Request) {
	// ...so we convert it to a string by passing it through
	// a buffer first. A 'costly' but useful process.
	var cmds TxnJSON
	if err := json.NewDecoder(r.Body).Decode(&cmds); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var raftCmds []*raftpb.Command
	for _, cmd := range cmds.Commands {
		raftCmds = append(raftCmds, &raftpb.Command{Method: cmd.Command, Key: cmd.Key, Value: cmd.Value})
	}

	_, err := s.coordinator.Transaction(raftCmds)
	if err != nil {
		s.log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)

}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
