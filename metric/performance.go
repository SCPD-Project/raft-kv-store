package main

import (
	"encoding/csv"
	"fmt"
	"github.com/raft-kv-store/client"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const coordAddr = "node0:17000"

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func TestGetLatency() {
	file, err := os.Create("get-metric.csv")
	checkError("Cannot create file", err)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, numClient := range []int{1, 2, 5, 10, 20, 50, 100, 150, 200} {
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
		time.Sleep(20 * time.Second)
	}
}

func main() {
	TestGetLatency()
}
