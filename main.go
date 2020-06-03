package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/coordinator"
	httpd "github.com/raft-kv-store/http"
	"github.com/raft-kv-store/store"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

const (
	DefaultListenAddress = "localhost:11000"
	DefaultRaftAddress   = "localhost:12000"

	DefaultCoordinatorListenAddress = "localhost:21000"
	DefaultCoordinatorRaftAddress   = "localhost:22000"
)

// Command line parameters
var (
	listenAddress     string
	raftAddress       string
	cohortRaftAddress string
	rpcAddress        string
	joinHTTPAddress   string
	raftDir           string
	nodeID            string
	bucketName        string
	isCoordinator     bool
)

func init() {

	flag.StringVarP(&listenAddress, "listen", "l", DefaultListenAddress, "Set the server listen address")

	flag.StringVarP(&raftAddress, "raft", "r", DefaultRaftAddress, "Set the RAFT binding address")
	flag.StringVarP(&cohortRaftAddress, "cohortRaft", "f", "", "Set the RAFT binding address")
	flag.StringVarP(&joinHTTPAddress, "join", "j", "", "Set joining HTTP address, if any")
	flag.StringVarP(&nodeID, "id", "i", "", "Node ID, randomly generated if not set")
	flag.StringVarP(&raftDir, "dir", "d", "", "Raft directory, ./$(nodeID) if not set")
	flag.IntVarP(&common.SnapshotInterval, "snapshotinterval", "", 30,
		"Snapshot interval in seconds, 30 seconds if not set")
	flag.IntVarP(&common.SnapshotThreshold, "snapshotthreshold", "", 5,
		"snapshot threshold of log indices, 5 if not set")
	flag.StringVarP(&bucketName, "bucketName/shard", "b", "", "Bucket name, randomly"+
		"generated if not set")
	flag.BoolVarP(&isCoordinator, "coordinator", "c", false, "Start as coordinator")

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
	log := logger.WithField("component", "main")

	if isCoordinator {
		c := coordinator.NewCoordinator(logger, nodeID, raftDir, raftAddress, joinHTTPAddress == "")
		h := httpd.NewService(logger, listenAddress, c)
		h.Start(joinHTTPAddress)
	} else {

		// derive raftaddress for cohort
		if cohortRaftAddress == "" {
			cohortRaftAddress = common.GetDerivedAddress(raftAddress)
		}
		kv := store.NewStore(logger, nodeID, raftDir, raftAddress, joinHTTPAddress == "", listenAddress, bucketName, cohortRaftAddress, joinHTTPAddress)
		kv.Start(joinHTTPAddress, nodeID)
	}

	log.Info("raftd started successfully")
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("raftd exiting")
}
