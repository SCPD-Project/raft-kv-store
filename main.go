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
	"strconv"
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
	snapshotInterval string
	snapshotThreshold string
	persistDir      string
)

func init() {
	flag.StringVarP(&listenAddress, "listen", "l", DefaultListenAddress, "Set the server listen address")
	flag.StringVarP(&raftAddress, "raft", "r", DefaultRaftAddress, "Set the RAFT binding address")
	flag.StringVarP(&joinHttpAddress, "join", "j", "", "Set joining HTTP address, if any")
	flag.StringVarP(&nodeID, "id", "i", "", "Node ID, randomly generated if not set")
	flag.StringVarP(&raftDir, "dir", "d", "", "Raft directory, ./$(nodeID) if not set")
	flag.StringVarP(&snapshotInterval, "snapshotinterval", "", "30",
		"Snapshot interval in seconds, 30 seconds if not set")
	flag.StringVarP(&snapshotThreshold, "snapshotthreshold", "", "5",
		"Snapshot threshold, 5 if not set")
	flag.StringVarP(&persistDir, "persistDir", "p", "raft", "Key-value persist directory")
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

	interval, err := strconv.Atoi(snapshotInterval); if err != nil {
		logger.WithField("component", "main").
			Fatalf(" Failed to parse snapshotinterval input: %s", err)
	}

	threshold, err := strconv.Atoi(snapshotThreshold); if err != nil {
		logger.WithField("component", "main").
			Fatalf(" Failed to parse snapshotthreshold input: %s", err)
	}

	persistInputs := store.PersistStateInputs{SnapshotInterval: interval,
					SnapshotThreshold: threshold,
					PersistDir: persistDir}

	kv := store.NewStore(logger, nodeID, raftAddress, raftDir, persistInputs)
	kv.Open(joinHttpAddress == "")

	h := httpd.NewService(logger, listenAddress, kv)
	h.Start(joinHttpAddress)

	logger.WithField("component", "main").Info("raftd started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	logger.WithField("component", "main").Info("raftd exiting")
}

