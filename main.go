package main

import (
	"fmt"
	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/store"
	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
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
	snapshotInterval int
	snapshotThreshold int
	bucketName      string
)

func init() {
	flag.StringVarP(&listenAddress, "listen", "l", DefaultListenAddress, "Set the server listen address")
	flag.StringVarP(&raftAddress, "raft", "r", DefaultRaftAddress, "Set the RAFT binding address")
	flag.StringVarP(&joinHttpAddress, "join", "j", "", "Set joining HTTP address, if any")
	flag.StringVarP(&nodeID, "id", "i", "", "Node ID, randomly generated if not set")
	flag.StringVarP(&raftDir, "dir", "d", "", "Raft directory, ./$(nodeID) if not set")
	flag.IntVarP(&snapshotInterval, "snapshotinterval", "", 30,
		"Snapshot interval in seconds, 30 seconds if not set")
	flag.IntVarP(&snapshotThreshold, "snapshotthreshold", "", 5,
		"snapshot threshold of log indices, 5 if not set")
	flag.StringVarP(&bucketName, "bucketName/shard", "b", "", "Bucket name, randomly" +
		"generated if not set")
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

	store.SnapshotInterval = snapshotInterval
	store.SnapshotThreshold = snapshotThreshold

	kv := store.NewStore(logger, nodeID, raftAddress, raftDir, bucketName)
	kv.Open(joinHttpAddress == "")

	h := httpd.NewService(logger, listenAddress, kv)
	h.Start(joinHttpAddress)

	logger.WithField("component", "main").Info("raftd started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	logger.WithField("component", "main").Info("raftd exiting")
}

