package main

import (
	"fmt"
	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/store"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"os/signal"
)

const (
	DefaultListenAddress = "localhost:11000"
	DefaultRaftAddress   = "localhost:12000"
)

// Command line parameters
var (
	listenAddress   string
	raftAddress     string
	joinHttpAddress string
	raftDir         string
	nodeID          string
	persistDir      string
)

func init() {
	flag.StringVarP(&listenAddress, "listen", "l", DefaultListenAddress, "Set the server listen address")
	flag.StringVarP(&raftAddress, "raft", "r", DefaultRaftAddress, "Set the RAFT binding address")
	flag.StringVarP(&joinHttpAddress, "join", "j", "", "Set joining HTTP address, if any")
	flag.StringVarP(&nodeID, "id", "i", "", "Node ID, randomly generated if not set")
	flag.StringVarP(&raftDir, "dir", "d", "", "Raft directory, ./$(nodeID) if not set")
	flag.StringVarP(&persistDir, "persistDir", "p", "raft", "Key-value persist directory")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	kv := store.NewStore(nodeID, raftAddress, raftDir, persistDir)

	kv.Open(joinHttpAddress == "")

	h := httpd.NewService(listenAddress, kv)
	h.Start(joinHttpAddress)

	log.Println("raftd started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("raftd exiting")

}

