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
	clientTimeout    = 5 * time.Second
	requestDuration  = 5 * time.Second
	coolDownDuration = 30 * time.Second
	getCSV           = "metric/metric-get.csv"
	setCSV           = "metric/metric-set-%d.csv"
	txnCSV           = "metric/metric-txn-%d-%s.csv"
	readOnlyCSV      = "metric/metric-read-%d-%s.csv"
	addCSV           = "metric/metric-add-%d.csv"
	xferCSV          = "metric/metric-xfer.csv"
	faultTolerance   = "metric/metric-fault-tol.csv"
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
			clients = append(clients, client.NewRaftKVClient(coordAddr, clientTimeout))
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
			clients = append(clients, client.NewRaftKVClient(coordAddr, clientTimeout))
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

func TestTxnLatency(filePath string, conflictRate int, singleShard, readOnly bool) {
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
			clients = append(clients, client.NewRaftKVClient(coordAddr, clientTimeout))
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
					var cmd string
					if readOnly {
						cmd = common.GET
					} else {
						cmd = common.SET
					}
					c.SetTxnCmd(&raftpb.RaftCommand{
						Commands: []*raftpb.Command{
							{Method: cmd, Key: key1, Value: int64(k + 1)},
							{Method: cmd, Key: key2, Value: int64(-k - 1)},
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

func TestAddLatency(filePath string, conflictRate int) {
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
			c := client.NewRaftKVClient(coordAddr, clientTimeout)
			clients = append(clients, c)
			c.Set(strconv.Itoa(i), int64(i))
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
					err := c.AddTransaction([]string{common.ADD, key, "1"})
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

func TestXferLatency(filePath string) {
	file, err := os.Create(filePath)
	checkError("Cannot create file", err)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for idx, numClient := range clientsToTest {
		offset := 100000
		title := fmt.Sprintf("client%d", numClient)
		log.Println(title)
		latencyRow := []string{title}
		latencies := make([][]int, numClient)
		var clients []*client.RaftKVClient
		for i := 0; i < numClient; i++ {
			c := client.NewRaftKVClient(coordAddr, clientTimeout)
			clients = append(clients, c)
			c.Set(strconv.Itoa(offset+i), int64(offset+i))
			c.Set(strconv.Itoa(i), int64(i))
		}
		var wg sync.WaitGroup
		for i, c := range clients {
			wg.Add(1)
			go func(c *client.RaftKVClient, k int) {
				defer wg.Done()
				expStart := time.Now()
				for time.Since(expStart) < requestDuration {
					start := time.Now()
					err := c.TransferTransaction([]string{common.TRANSFER, strconv.Itoa(offset + k), strconv.Itoa(k), "1"})
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

type timeStamp struct {
	start   int
	end     int
	err     string
	op      string
}

func TestFaultTolerance(filePath string, numClient int, getPercent float32) {
	file, err := os.Create(filePath)
	checkError("Cannot create file", err)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	timestampCol := []string{"start", "end", "error", "op"}
	err = writer.Write(timestampCol)
	checkError("Cannot write to file", err)
	timestamps := make([][]timeStamp, numClient)

	var clients []*client.RaftKVClient
	for i := 0; i < numClient; i++ {
		c := client.NewRaftKVClient(coordAddr, clientTimeout)
		clients = append(clients, c)
	}
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(c *client.RaftKVClient, k int) {
			defer wg.Done()
			expStart := time.Now()
			key := strconv.Itoa(k)
			for time.Since(expStart) <60*time.Second {
				var start, end time.Duration
				//var success bool
				var op, errStr string
				if float32(k)/float32(numClient) < getPercent {
					start = time.Since(expStart)
					err := c.Get(key)
					end = time.Since(expStart)
					if err != nil && err.Error() != fmt.Sprintf("Key=%s does not exist", key) {
						errStr = err.Error()
					}
					op = common.GET
				} else {
					start = time.Since(expStart)
					err := c.Set(key, int64(k))
					end = time.Since(expStart)
					if err != nil {
						errStr = err.Error()
					}
					op = common.SET
				}
				timestamps[k] = append(
					timestamps[k],
					timeStamp{
						start:   int(start) / int(time.Nanosecond),
						end:     int(end) / int(time.Nanosecond),
						err: errStr,
						op:      op,
					},
				)
			}
		}(c, i)
	}
	wg.Wait()
	for _, tt := range timestamps {
		for _, t := range tt {
			timestampCol = []string{strconv.Itoa(t.start), strconv.Itoa(t.end), t.err, t.op}
			err := writer.Write(timestampCol)
			checkError("Cannot write to file", err)
		}
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
	TestTxnLatency(txnCSV, 0, false, false)
	TestTxnLatency(readOnlyCSV, 0, false, true)
	for _, i := range []int{0, 2, 5, 10} {
		TestAddLatency(addCSV, i)
	}
	TestXferLatency(xferCSV)
	TestFaultTolerance(faultTolerance, 5, 0.8)
}
