package http

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
)

const (
	GET = "GET"
	POST = "POST"
	DELETE = "DELETE"
)

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	msg, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("Error when reading join data", err)
	}

	var joinMsg raftpb.JoinMsg
	if err = proto.Unmarshal(msg, &joinMsg); err != nil {
		log.Fatal("Error when unmarshal join data", err)
	}

	if joinMsg.RaftAddress == "" || joinMsg.ID == ""{
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(joinMsg.ID, joinMsg.RaftAddress); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func(path string) string {
		parts := strings.SplitN(path, "/", 3)
		if len(parts) != 3 {
			log.Fatalf("Error in getting key from %s", r.URL.Path)
		}
		return parts[2]
	}

	switch r.Method {
	case GET:
		key := getKey(r.URL.Path)
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		val, err := s.store.Get(key)
		if err != nil {
			io.WriteString(w, err.Error() + "\n")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, fmt.Sprintf("Key: %s, Value: %s\n", key, val))
		w.WriteHeader(http.StatusAccepted)

	case POST:
		// Read the value from the POST body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				log.Printf("Unable to set :%s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)

	case DELETE:
		key := getKey(r.URL.Path)
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.store.Delete(key); err != nil {
			log.Print(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.store.Delete(key)
		w.WriteHeader(http.StatusAccepted)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

func (s *Service) handleLeader(w http.ResponseWriter, r *http.Request) {
	log.Print("Handling request for leader")
	io.WriteString(w, s.store.Leader())
	w.WriteHeader(http.StatusAccepted)
}

// handleTransaction
func (s *Service) handleTransaction(w http.ResponseWriter, r *http.Request) {
	// ...so we convert it to a string by passing it through
	// a buffer first. A 'costly' but useful process.
	var cmds  struct {
			Commands []struct {
				Command string `json:"Command"`
				Key     string `json:"Key"`
				Value   string `json:"Value"`
			}
	}
	if err := json.NewDecoder(r.Body).Decode(&cmds); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if r.Method != POST {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var raftCmds []*raftpb.Command
	for _, cmd := range cmds.Commands {
		raftCmds = append(raftCmds, &raftpb.Command{Method: cmd.Command, Key: cmd.Key, Value: cmd.Value})
	}
	log.Print(raftCmds)
	err := s.store.Transaction(raftCmds)
	if err != nil {
		log.Print(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)

}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

