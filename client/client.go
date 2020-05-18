package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	httpd "github.com/RAFT-KV-STORE/http"
	"github.com/RAFT-KV-STORE/raftpb"
)

func addURLScheme(s string) string{
	if strings.HasPrefix(s, "https://") {
		s = strings.Replace(s, "https://", "http://", 1)
		return s
	} else if !strings.HasPrefix(s, "http://") {
		return  "http://" + s
	}
	return s
}

type raftKVClient struct{
	client *http.Client
	serverAddr string
	Terminate chan os.Signal
	reader *bufio.Reader
	inTxn bool
	txnCmds httpd.TxnJSON
}

func NewRaftKVClient(serverAddr string) *raftKVClient{
	c := &raftKVClient{
		client: &http.Client{Timeout: 5 * time.Second},
		serverAddr: addURLScheme(serverAddr),
		Terminate : make(chan os.Signal, 1),
		reader: bufio.NewReader(os.Stdin),
	}
	return c
}

func (c *raftKVClient) setServerAddr(newAddr string) {
	c.serverAddr = addURLScheme(newAddr)
}

func (c *raftKVClient) readString() []string {
	var cmdArr []string
	fmt.Print(">")
	cmdStr, err := c.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	cmdStr=strings. TrimSuffix(cmdStr, "\n")
	// To gather quotes
	cmdArr = regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)\n`).FindAllString(cmdStr, -1)
	for i := range cmdArr {
		cmdArr[i] = strings.Trim(cmdArr[i], "'\"")
	}
	return cmdArr
}

func (c *raftKVClient) validGet(cmdArr []string) bool {
	if len(cmdArr) != 2 {
		fmt.Println("Invalid get command. Correct syntax: get [key]")
		return false
	}
	return true
}

func (c *raftKVClient) validSet(cmdArr []string) bool {
	if len(cmdArr) != 3 {
		fmt.Println("Invalid set command. Correct syntax: set [key] [value]")
		return false
	}
	return true
}

func (c *raftKVClient) validDel(cmdArr []string) bool {
	if len(cmdArr) != 2 {
		fmt.Println("Invalid delete command. Correct syntax: del [key]")
		return false
	}
	return true
}

func (c *raftKVClient) validTxn(cmdArr []string) bool {
	if c.inTxn {
		fmt.Println("Already in transaction")
		return false
	}
	if len(cmdArr) != 1 {
		fmt.Println("Invalid transaction command. Correct syntax: txn")
		return false
	}
	return true
}

func (c *raftKVClient) validEndTxn(cmdArr []string) bool {
	if !c.inTxn {
		fmt.Println("Not in transaction")
		return false
	}
	if len(cmdArr) != 1 {
		fmt.Println("Invalid end transaction command. Correct syntax: end")
		return false
	}
	return true
}

func (c *raftKVClient) validExit(cmdArr []string) bool {
	if len(cmdArr) != 1 {
		fmt.Println("Invalid exit command. Correct syntax: exit")
		return false
	}
	return true
}

func (c *raftKVClient) validCmd(cmdArr []string) bool {
	if len(cmdArr) == 0 {
		return false
	}
	switch cmdArr[0] {
	case raftpb.GET:
		return c.validGet(cmdArr)
	case raftpb.SET:
		return c.validSet(cmdArr)
	case raftpb.DEL:
		return c.validDel(cmdArr)
	case raftpb.TXN:
		return c.validTxn(cmdArr)
	case raftpb.ENDTXN:
		return c.validEndTxn(cmdArr)
	case raftpb.EXIT:
		return c.validExit(cmdArr)
	default:
		fmt.Println("Command not recognized.")
		return false
	}
}

func (c *raftKVClient) TransactionRun(cmdArr []string) {
	switch cmdArr[0] {
	case raftpb.TXN:
		c.inTxn = true
		c.txnCmds = httpd.TxnJSON{}
		fmt.Println("Entering transaction status")
	case raftpb.GET:
		fmt.Println("Only set and delete command are available in transaction.")
	case raftpb.SET:
		c.txnCmds.Commands = append(c.txnCmds.Commands, httpd.TxnCommand{
			Command: raftpb.SET,
			Key: cmdArr[1],
			Value: cmdArr[2],
		})
	case raftpb.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, httpd.TxnCommand{
			Command: raftpb.DEL,
			Key: cmdArr[1],
		})
	case raftpb.ENDTXN:
		if err := c.Transaction(); err != nil {
			fmt.Println(err)
		}
		c.inTxn = false
	case raftpb.EXIT:
		fmt.Println("Stop client")
		os.Exit(0)
	}
}

func (c *raftKVClient) Run() {
	for {
		cmdArr := c.readString()
		if !c.validCmd(cmdArr) {
			continue
		}
		if c.inTxn {
			c.TransactionRun(cmdArr)
			continue
		}
		switch cmdArr[0] {
		case raftpb.GET:
			if err := c.Get(cmdArr[1]); err != nil {
				fmt.Println(err)
			}
		case raftpb.SET:
			if err := c.Set(cmdArr[1], cmdArr[2]); err != nil {
				fmt.Println(err)
			}
		case raftpb.DEL:
			if err := c.Delete(cmdArr[1]); err != nil {
				fmt.Println(err)
			}
		case raftpb.TXN:
			c.TransactionRun(cmdArr)
		case raftpb.EXIT:
			fmt.Println("Stop client")
			os.Exit(0)
		}
	}
}

func (c *raftKVClient) parseServerAddr(key string) (string, error){
	u, err := url.Parse(c.serverAddr)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "key", key)
	return u.String(), nil
}

func (c *raftKVClient) newRequest(method, key string, data []byte) (*http.Response, error) {
	url, err := c.parseServerAddr(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *raftKVClient) newTxnRequest(data []byte) (*http.Response, error) {
	u, err := url.Parse(c.serverAddr)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "transaction")
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *raftKVClient) Get(key string) error {
	resp, err := c.newRequest(http.MethodGet, key, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf(string(body))
}

func (c *raftKVClient) Set(key string, value string) error{
	var reqBody []byte
	var err error
	if reqBody, err = json.Marshal(httpd.SetJSON{key: value}); err != nil {
		return err
	}
	resp, err := c.newRequest(http.MethodPost, key, reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
		return nil
	}
	return fmt.Errorf(string(body))
}

func (c *raftKVClient) Delete(key string) error {
	resp, err := c.newRequest(http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
		return nil
	}
	return fmt.Errorf(string(body))
}

func (c *raftKVClient) Transaction() error{
	fmt.Printf("Sumbitting %s\n", c.txnCmds)
	var reqBody []byte
	var err error
	if reqBody, err = json.Marshal(c.txnCmds); err != nil {
		return err
	}
	resp, err := c.newTxnRequest(reqBody)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
		return nil
	}
	return fmt.Errorf(string(body))
}