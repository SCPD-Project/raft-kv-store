package main

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/raft-kv-store/client"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
	flag "github.com/spf13/pflag"
)

const (
	// FIXME: this won't work after client finding leader PR merged when running locally
	localCoordAddr   = "127.0.0.1:17000"
	dockerCoordAddr  = "node0:17000"
	requestDuration  = 5 * time.Second
	coolDownDuration = 30 * time.Second
	getCSV           = "metric/metric-get.csv"
	setCSV           = "metric/metric-set-%d.csv"
	txnCSV           = "metric/metric-txn-%d-%s.csv"
)

var (
	clientsToTest = []int{1, 2, 5, 10, 20, 50, 100, 150}
	coordAddr     string
	inContainer   bool
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

func TestGetLatency(filePath string) {
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
				for time.Since(expStart) < requestDuration {
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
		coolDown(clientsToTest, idx)
	}
}

func TestSetLatency(filePath string, conflictRate int) {
	file, err := os.Create(fmt.Sprintf(filePath, conflictRate))
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
				expStart := time.Now()
				for time.Since(expStart) < requestDuration {
					var key string
					if conflictRate == 0 {
						key = strconv.Itoa(k)
					} else {
						key = fmt.Sprint(intToHash(k) % (numClient/conflictRate + 1))
					}
					start := time.Now()
					err := c.Set(key, int64(k))
					latencies[k] = append(latencies[k], int(time.Since(start)/time.Microsecond))
					if err != nil {
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
		coolDown(clientsToTest, idx)
	}
}

func TestTxnConflictLatency(filePath string, conflictRate int, singleShard bool) {
	var mode string
	if singleShard {
		mode = "single"
	} else {
		mode = "multiple"
	}
	file, err := os.Create(fmt.Sprintf(filePath, conflictRate, mode))
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
				expStart := time.Now()
				for time.Since(expStart) < requestDuration {
					var shard1, shard2 int64
					var key1, key2 string
					offset := 1000000
					if conflictRate == 0 {
						key1 = fmt.Sprint(k)
					} else {
						key1 = fmt.Sprint(intToHash(k) % (numClient/conflictRate + 1))
					}
					shard1 = common.SimpleHash(key1, 2)
					for {
						if conflictRate == 0 {
							key2 = fmt.Sprint(k + offset)
						} else {
							key2 = fmt.Sprint(intToHash(k+offset)%numClient/conflictRate + 1)
						}
						shard2 = common.SimpleHash(key1, 2)
						if !singleShard || shard1 == shard2 {
							break
						}
						offset++
					}
					c.SetTxnCmd(&raftpb.RaftCommand{
						Commands: []*raftpb.Command{
							{Method: common.SET, Key: key1, Value: int64(k + 1)},
							{Method: common.SET, Key: key2, Value: int64(-k - 1)},
						},
						IsTxn: true,
					})
					start := time.Now()
					_, err := c.Transaction()
					latencies[k] = append(latencies[k], int(time.Since(start)/time.Microsecond))
					if err != nil {
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
		coolDown(clientsToTest, idx)
	}
}

func coolDown(clientsToTest []int, idx int) {
	if idx != len(clientsToTest)-1 {
		fmt.Printf("Waiting %s to cool down for %d clients test...\n", coolDownDuration, clientsToTest[idx+1])
		time.Sleep(coolDownDuration)
	} else {
		fmt.Println("Finished")
	}
}

func intToHash(k int) int {
	h1 := fnv.New32a()
	h1.Write([]byte(strconv.Itoa(k)))
	return int(h1.Sum32())
}

func main() {
	flag.Parse()
	if inContainer {
		coordAddr = dockerCoordAddr
	} else {
		coordAddr = localCoordAddr
	}
	TestGetLatency(getCSV)
	for _, i := range []int{0, 2, 5, 10} {
		TestSetLatency(setCSV, i)
	}
	TestTxnConflictLatency(txnCSV, 0, true)
}