package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/raft-kv-store/client"
	flag "github.com/spf13/pflag"
)

const (
	DefaultServerAddress = "localhost:11000"
)

// Command line parameters
var (
	serverAddress string
)

func init() {
	flag.StringVarP(&serverAddress, "endpoint", "e", DefaultServerAddress, "Set the endpoint address")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	c := client.NewRaftKVClient(serverAddress, 2 * time.Second)
	c.Run()
	signal.Notify(c.Terminate, os.Interrupt)
	<-c.Terminate
}
