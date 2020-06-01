package http

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/raft-kv-store/raftpb"
)

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

	var msg string
	if !s.coordinator.IsLeader() {
		msg = "Not a leader"
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, msg)
		return
	}

	getKey := func(path string) string {
		parts := strings.SplitN(path, "/", 3)
		if len(parts) != 3 {
			s.log.Fatalf("Error in getting key from %s", r.URL.Path)
		}
		return parts[2]
	}

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
			msg = fmt.Sprintf("Key=%s, Value=%d", key, val)
		}
		io.WriteString(w, msg)

	case http.MethodPost:
		cmd := &raftpb.Command{}
		if m, err := ioutil.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg = fmt.Sprintf("failed to read %v", r.Body)
		} else if err = proto.Unmarshal(m, cmd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg = fmt.Sprintf("failed to parse %v", r.Body)
		} else if err := s.coordinator.Set(cmd.Key, cmd.Value); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg = fmt.Sprintf("Unable to set: %s", err.Error())
		} else {
			w.WriteHeader(http.StatusOK)
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
}

// TODO: No raft leader api exposed in coordinator
// // handleLeader mainly used for debugs.
// func (s *Service) handleLeader(w http.ResponseWriter, r *http.Request) {

// 	s.log.Debug("Handling request for leader")
// 	io.WriteString(w, string(s.coordinator.Leader()))

// }

// handleTransaction
func (s *Service) handleTransaction(w http.ResponseWriter, r *http.Request) {
	var msg string

	if !s.coordinator.IsLeader() {
		msg = "Not a leader"
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, msg)
		return
	}

	// ...so we convert it to a string by passing it through
	// a buffer first. A 'costly' but useful process.
	cmds := &raftpb.RaftCommand{}
	if m, err := ioutil.ReadAll(r.Body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		msg = fmt.Sprintf("failed to read %v", r.Body)
	} else if err = proto.Unmarshal(m, cmds); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		msg = fmt.Sprintf("failed to parse %v", r.Body)
	} else if resultCmds, err := s.coordinator.Transaction(cmds); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		msg = fmt.Sprintf("Unable to txn: %s", err.Error())
	} else if respBody, err := proto.Marshal(resultCmds); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		msg = fmt.Sprintf("Unable to marshal: %s", err.Error())
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(respBody)
	}
	if msg != "" {
		s.log.Info(msg)
		io.WriteString(w, msg)
	}
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
