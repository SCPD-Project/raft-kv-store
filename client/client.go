package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/RAFT-KV-STORE/common"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/RAFT-KV-STORE/raftpb"
	"github.com/golang/protobuf/proto"
)

var(
	CmdRegex = regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)\n`)
)

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

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
	// TODO: Add stop API to avoid exposing Terminate channel
	Terminate chan os.Signal
	reader *bufio.Reader
	inTxn bool
	txnCmds *raftpb.RaftCommand
}

func NewRaftKVClient(serverAddr string) *raftKVClient{
	c := &raftKVClient{
		client: &http.Client{Timeout: 5 * time.Second},
		serverAddr: addURLScheme(serverAddr),
		Terminate : make(chan os.Signal, 1),
		reader: bufio.NewReader(os.Stdin),
		txnCmds : &raftpb.RaftCommand{},
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
	cmdArr = CmdRegex.FindAllString(cmdStr, -1)
	for i := range cmdArr {
		cmdArr[i] = strings.Trim(cmdArr[i], "'\"")
	}
	return cmdArr
}

func (c *raftKVClient) validCmd2(cmdArr []string, op string) error {
	if len(cmdArr) != 2 {
		return fmt.Errorf("Invalid %[1]s command. Correct syntax: %[1]s [key]", op)
	}
	return nil
}

func (c *raftKVClient) validCmd3(cmdArr []string, op string) error {
	if len(cmdArr) != 3{
		return fmt.Errorf("Invalid %[1]s command. Correct syntax: %[1]s [key] [value]", op)
	}
	if _, ok := parseInt64(cmdArr[2]); ok != nil{
		return fmt.Errorf("Invalid %s command. Error in parsing %s as numerical value", op, cmdArr[2])
	}
	return nil
}

func (c *raftKVClient) validTxn(cmdArr []string) error {
	if c.inTxn {
		return errors.New("Already in transaction")
	}
	if len(cmdArr) != 1 {
		return errors.New("Invalid transaction command. Correct syntax: txn")
	}
	return nil
}

func (c *raftKVClient) validEndTxn(cmdArr []string) error {
	if !c.inTxn {
		return errors.New("Not in transaction")
	}
	if len(cmdArr) != 1 {
		return errors.New("Invalid end transaction command. Correct syntax: end")
	}
	return nil
}

func (c *raftKVClient) validExit(cmdArr []string) error {
	if len(cmdArr) != 1 {
		return errors.New("Invalid exit command. Correct syntax: exit")
	}
	return nil
}

func (c *raftKVClient) validCmd(cmdArr []string) error {
	if len(cmdArr) == 0 {
		return errors.New("")
	}
	switch cmdArr[0] {
	case common.GET, common.DEL:
		return c.validCmd2(cmdArr, cmdArr[0])
	case common.SET, common.ADD, common.SUB:
		return c.validCmd3(cmdArr, cmdArr[0])
	case common.TXN:
		return c.validTxn(cmdArr)
	case common.ENDTXN:
		return c.validEndTxn(cmdArr)
	case common.EXIT:
		return c.validExit(cmdArr)
	default:
		return errors.New("Command not recognized.")
	}
}

func (c *raftKVClient) TransactionRun(cmdArr []string) {
	switch cmdArr[0] {
	case common.TXN:
		c.inTxn = true
		c.txnCmds = &raftpb.RaftCommand{}
		fmt.Println("Entering transaction status")
	case common.GET:
		fmt.Println("Only set and delete command are available in transaction.")
	case common.SET:
		val, _ := parseInt64(cmdArr[2])
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: common.SET,
			Key: cmdArr[1],
			Value: val,
		})
	case common.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: common.DEL,
			Key: cmdArr[1],
		})
	case common.ADD, common.SUB:
		fmt.Println("Not implemented")
	case common.ENDTXN:
		if err := c.Transaction(); err != nil {
			fmt.Println(err)
		}
		c.inTxn = false
	case common.EXIT:
		fmt.Println("Stop client")
		os.Exit(0)
	}
}

func (c *raftKVClient) Run() {
	for {
		cmdArr := c.readString()
		if err := c.validCmd(cmdArr); err != nil{
			if err.Error() != "" {
				fmt.Println(err)
			}
			continue
		}
		if c.inTxn {
			c.TransactionRun(cmdArr)
			continue
		}
		switch cmdArr[0] {
		case common.GET:
			if err := c.Get(cmdArr[1]); err != nil {
				fmt.Println(err)
			}
		case common.SET:
			val, _ := strconv.ParseInt(cmdArr[2], 10, 64)
			if err := c.Set(cmdArr[1], val); err != nil {
				fmt.Println(err)
			}
		case common.DEL:
			if err := c.Delete(cmdArr[1]); err != nil {
				fmt.Println(err)
			}
		case common.ADD, common.SUB:
			fmt.Println("Not implemented")
		case common.TXN:
			c.TransactionRun(cmdArr)
		case common.EXIT:
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
	return errors.New(string(body))
}

func (c *raftKVClient) Set(key string, value int64) error{
	var reqBody []byte
	var err error
	if reqBody, err = proto.Marshal(&raftpb.Command{
		Method: common.SET,
		Key: key,
		Value: value,
	}); err != nil {
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
	return errors.New(string(body))
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
	return errors.New(string(body))
}

func (c *raftKVClient) Transaction() error{
	fmt.Printf("Submitting %v\n", c.txnCmds)
	var reqBody []byte
	var err error
	if reqBody, err = proto.Marshal(c.txnCmds); err != nil {
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
	return errors.New(string(body))
}