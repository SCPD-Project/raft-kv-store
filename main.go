package main

import (
	"fmt"
	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/store"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	nested "github.com/antonfisher/nested-logrus-formatter"
	"os"
	"os/signal"
	"path"
	"runtime"
	"trings"
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
)

func init() {
	flag.StringVarP(&listenAddress, "listen", "l", DefaultListenAddress, "Set the server listen address")
	flag.StringVarP(&raftAddress, "raft", "r", DefaultRaftAddress, "Set the RAFT binding address")
	flag.StringVarP(&joinHttpAddress, "join", "j", "", "Set joining HTTP address, if any")
	flag.StringVarP(&nodeID, "id", "i", "", "Node ID, randomly generated if not set")
	flag.StringVarP(&raftDir, "dir", "d", "", "Raft directory, ./$(nodeID) if not set")
	flag.Usage = func() {
		log.Errorf("Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	logger := log.New()
	logger.SetFormatter(&nested.Formatter{
		HideKeys:    true,
		FieldsOrder: []string{"component"},
		CustomCallerFormatter: func(f *runtime.Frame) string {
			s := strings.Split(f.Function, ".")
			funcName := s[len(s)-1]
			return fmt.Sprintf(" [%s:%d][%s()]", path.Base(f.File), f.Line, funcName)
		},
		CallerFirst: true,
	})

	logger.SetReportCaller(true)

	kv := store.NewStore(logger, nodeID, raftAddress, raftDir)
	kv.Open(joinHttpAddress == "")

	h := httpd.NewService(logger, listenAddress, kv)
	h.Start(joinHttpAddress)

	logger.WithField("component", "main").Info("raftd started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	logger.WithField("component", "main").Info("raftd exiting")
}

