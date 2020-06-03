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

	"github.com/gogo/protobuf/proto"
	"github.com/raft-kv-store/coordinator"
	"github.com/raft-kv-store/raftpb"
)

// Service provides HTTP service.
type Service struct {
	addr        string
	ln          net.Listener
	log         *log.Entry
	coordinator *coordinator.Coordinator
}

// NewService returns an uninitialized HTTP service.
func NewService(logger *log.Logger, addr string, coordinator *coordinator.Coordinator) *Service {

	l := logger.WithField("component", "http")

	return &Service{
		addr:        addr,
		coordinator: coordinator,
		log:         l,
	}
}

// Start starts the service.
func (s *Service) Start(joinHTTPAddress string) {
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

	if joinHTTPAddress != "" {
		msg := &raftpb.JoinMsg{RaftAddress: s.coordinator.RaftAddress, ID: s.coordinator.ID}
		b, err := proto.Marshal(msg)
		if err != nil {
			s.log.Fatalf("error when marshaling %+v", msg)
		}
		resp, err := http.Post(fmt.Sprintf("http://%s/join", joinHTTPAddress), "application/protobuf", bytes.NewBuffer(b))
		if err != nil {
			s.log.Fatalf("failed to join %s: %s", joinHTTPAddress, err)
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
	s.log.Infof("Serving request for path: %s\n", r.URL.Path)
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/transaction") {
		s.handleTransaction(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}
