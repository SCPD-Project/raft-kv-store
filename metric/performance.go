package main

import (
	"encoding/csv"
	"fmt"
	"github.com/raft-kv-store/client"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	localCoordAddr = "127.0.0.1:17000"
	dockerCoordAddr = "node0:17000"
)

var (
	clientsToTest = []int{1, 2, 5, 10, 20, 50, 100, 200}
	coordAddr string
	inContainer bool
)


func init() {
	flag.BoolVarP(&inContainer, "container", "c", false, "Run test in container")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func TestGetLatency() {
	filePath := "metric/get-metric.csv"
	file, err := os.Create(filePath)
	checkError("Cannot create file", err)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for idx, numClient := range clientsToTest {
		title := fmt.Sprintf("client%d", numClient)
		log.Println(title)
		latencyRow := []string{title}
		latencies := make([][]int, numClient)
		var clients []*client.RaftKVClient
		for i := 0; i < numClient; i++ {
			clients = append(clients, client.NewRaftKVClient(coordAddr))
		}
		var wg sync.WaitGroup
		for i, c := range clients {
			wg.Add(1)
			go func(c *client.RaftKVClient, k int) {
				defer wg.Done()
				key := strconv.Itoa(k)
				expStart := time.Now()
				for time.Since(expStart) < 5*time.Second {
					start := time.Now()
					err := c.Get(key)
					if err == nil || err.Error() == fmt.Sprintf("Key=%s does not exist", key) {
						latencies[k] = append(latencies[k], int(time.Since(start)/time.Microsecond))
					} else {
						fmt.Println(err)
					}
				}
			}(c, i)
		}
		wg.Wait()
		for _, lat := range latencies {
			for _, l := range lat {
				latencyRow = append(latencyRow, strconv.Itoa(l))
			}
		}
		err := writer.Write(latencyRow)
		checkError("Cannot write to file", err)
		if idx != len(clientsToTest) - 1 {
			fmt.Println("Waiting 20 sec to cool down")
		} else {
			fmt.Println("Finished")
		}

		time.Sleep(20 * time.Second)
	}
}

func main() {
	flag.Parse()
	if inContainer {
		coordAddr = dockerCoordAddr
	} else {
		coordAddr = localCoordAddr
	}
	TestGetLatency()
}
