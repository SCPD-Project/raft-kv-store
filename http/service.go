// Package http provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/RAFT-KV-STORE/coordinator"
	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/RAFT-KV-STORE/store"
	"github.com/gogo/protobuf/proto"
)

// Service provides HTTP service.
type Service struct {
	addr        string
	ln          net.Listener
	log         *log.Entry
	store       *store.Store
	coordinator *coordinator.Coordinator
}

// NewService returns an uninitialized HTTP service.
func NewService(logger *log.Logger, addr string, store *store.Store, coordinator *coordinator.Coordinator) *Service {

	l := logger.WithField("component", "http")
	if store == nil && coordinator == nil {
		log.Fatalf("Invalid config, either kv or coordinator has to be initialized")
	}

	return &Service{
		addr:        addr,
		store:       store,
		coordinator: coordinator,
		log:         l,
	}
}

// Start starts the service.
func (s *Service) Start(joinHttpAddress string) {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatalf("failed to start HTTP service: %s", err.Error())
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			s.log.Fatalf("HTTP serve error: %s", err)
		}
	}()

	var raftAddress, id string
	if s.store != nil {
		raftAddress = s.store.RaftAddress
		id = s.store.ID
	} else {
		raftAddress = s.coordinator.RaftAddress
		id = s.coordinator.ID
	}

	if joinHttpAddress != "" {
		msg := &raftpb.JoinMsg{RaftAddress: raftAddress, ID: id}
		b, err := proto.Marshal(msg)
		if err != nil {
			s.log.Fatalf("error when marshaling %+v", msg)
		}
		resp, err := http.Post(fmt.Sprintf("http://%s/join", joinHttpAddress), "application/protobuf", bytes.NewBuffer(b))
		if err != nil {
			s.log.Fatalf("failed to join %s: %s", joinHttpAddress, err)
		}
		defer resp.Body.Close()
	}
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// all the requests except join go to co-ordinator.
	if r.URL.Path == "/join" {
		s.handleJoin(w, r)
		return
	}

	fmt.Printf("Serving request for path: %s\n", r.URL.Path)
	if s.coordinator != nil {
		if strings.HasPrefix(r.URL.Path, "/key") {
			s.handleKeyRequest(w, r)
		} else if strings.HasPrefix(r.URL.Path, "/leader") {
			s.handleLeader(w, r)
		} else if strings.HasPrefix(r.URL.Path, "/transaction") {
			s.handleTransaction(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}
