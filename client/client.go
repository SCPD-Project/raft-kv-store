package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
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

	"github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/raft-kv-store/common"
	"github.com/raft-kv-store/raftpb"
)

var (
	CmdRegex = regexp.MustCompile(`[^\s"']+|"([^"]*)"|'([^']*)\n`)
)

const maxTransferRetries = 5

type insufficientFundsError struct {
	key            string
	remain, expect int64
}

func (err *insufficientFundsError) Error() string {
	return fmt.Sprintf("Insufficient funds: %d < %d in %s", err.remain, err.expect, err.key)
}

/*type serverUnavailableError struct {
	err            string
}

func (err *serverUnavailableError) Error() string {
	return fmt.Sprintf("err: %s", err)
}*/

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func addURLScheme(s string) string {
	if strings.HasPrefix(s, "https://") {
		s = strings.Replace(s, "https://", "http://", 1)
		return s
	} else if !strings.HasPrefix(s, "http://") {
		return "http://" + s
	}
	return s
}

type RaftKVClient struct {
	client     *http.Client
	serverAddr string
	// TODO: Add stop API to avoid exposing Terminate channel
	Terminate chan os.Signal
	reader    *bufio.Reader
	inTxn     bool
	txnCmds   *raftpb.RaftCommand
}

func NewRaftKVClient(serverAddr string, timeout time.Duration) *raftKVClient {
	c := &RaftKVClient{
		client:     &http.Client{Timeout: timeout},
		serverAddr: addURLScheme(serverAddr),
		Terminate:  make(chan os.Signal, 1),
		reader:     bufio.NewReader(os.Stdin),
		txnCmds:    &raftpb.RaftCommand{},
	}
	return c
}

func (c *RaftKVClient) setServerAddr(newAddr string) {
	c.serverAddr = addURLScheme(newAddr)
}

func (c *RaftKVClient) readString() []string {
	var cmdArr []string
	fmt.Print(">")
	cmdStr, err := c.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	cmdStr = strings.TrimSuffix(cmdStr, "\n")
	// To gather quotes
	cmdArr = CmdRegex.FindAllString(cmdStr, -1)
	for i := range cmdArr {
		cmdArr[i] = strings.Trim(cmdArr[i], "'\"")
	}
	return cmdArr
}

func (c *RaftKVClient) validCmd2(cmdArr []string) error {
	if len(cmdArr) != 2 {
		return fmt.Errorf("Invalid %[1]s command. Correct syntax: %[1]s [key]", cmdArr[0])
	}
	return nil
}

func (c *RaftKVClient) validCmd3(cmdArr []string) error {
	if len(cmdArr) != 3 {
		return fmt.Errorf("Invalid %[1]s command. Correct syntax: %[1]s [key] [value]", cmdArr[0])
	}
	if _, ok := parseInt64(cmdArr[2]); ok != nil {
		return fmt.Errorf("Invalid %s command. Error in parsing %s as numerical value", cmdArr[0], cmdArr[2])
	}
	return nil
}

func (c *RaftKVClient) validTxn(cmdArr []string) error {
	if c.inTxn {
		return errors.New("Already in transaction")
	}
	if len(cmdArr) != 1 {
		return errors.New("Invalid transaction command. Correct syntax: txn")
	}
	return nil
}

func (c *RaftKVClient) validEndTxn(cmdArr []string) error {
	if !c.inTxn {
		return errors.New("Not in transaction")
	}
	if len(cmdArr) != 1 {
		return errors.New("Invalid end transaction command. Correct syntax: end")
	}
	return nil
}

func (c *RaftKVClient) validExit(cmdArr []string) error {
	if len(cmdArr) != 1 {
		return errors.New("Invalid exit command. Correct syntax: exit")
	}
	return nil
}

// Simpler version of 'transfer' command, eg., Issuing `transfer x y 10` is translated
// to `transfer 10 from x to y`
func (c *RaftKVClient) validTxnTransfer(cmdArr []string) error {
	if len(cmdArr) != 4 {
		return fmt.Errorf("invalid %[1]s command. Correct syntax: %[1]s [fromKey] [toKey] "+
			"[amount to be transferred]", cmdArr[0])
	}

	if _, ok := parseInt64(cmdArr[3]); ok != nil {
		return fmt.Errorf("invalid %s command. Error in parsing %s as numerical value", cmdArr[0], cmdArr[3])
	}

	return nil
}

func (c *RaftKVClient) validCmd(cmdArr []string) error {
	if len(cmdArr) == 0 {
		return errors.New("")
	}
	switch cmdArr[0] {
	case common.GET, common.DEL:
		return c.validCmd2(cmdArr)
	case common.SET, common.ADD, common.SUB:
		return c.validCmd3(cmdArr)
	case common.TXN:
		return c.validTxn(cmdArr)
	case common.ENDTXN:
		return c.validEndTxn(cmdArr)
	case common.EXIT:
		return c.validExit(cmdArr)
	case common.TRANSFER:
		return c.validTxnTransfer(cmdArr)
	default:
		return errors.New("Command not recognized.")
	}
}

func (c *RaftKVClient) TransactionRun(cmdArr []string) {
	switch cmdArr[0] {
	case common.TXN:
		c.inTxn = true
		c.txnCmds = &raftpb.RaftCommand{}
		fmt.Println("Entering transaction status")
	case common.SET:
		val, _ := parseInt64(cmdArr[2])
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: common.SET,
			Key:    cmdArr[1],
			Value:  val,
		})
	case common.DEL:
		c.txnCmds.Commands = append(c.txnCmds.Commands, &raftpb.Command{
			Method: common.DEL,
			Key:    cmdArr[1],
		})
	case common.ENDTXN:
		if _, err := c.Transaction(); err != nil {
			fmt.Println(err)
		}
		c.inTxn = false
	case common.EXIT:
		fmt.Println("Stop client")
		os.Exit(0)
	default:
		fmt.Println("Only set and delete command are available in transaction.")
	}
}

func (c *RaftKVClient) TransferTransaction(cmdArr []string) error {
	fromKey := cmdArr[1]
	toKey := cmdArr[2]
	if fromKey == toKey {
		return fmt.Errorf("Invalid transfer for same key %s", fromKey)
	}

	transferAmount, _ := parseInt64(cmdArr[3])
	var err error

	if transferAmount == 0 {
		return fmt.Errorf("Invalid transfer amount %d, so aborting the txn", transferAmount)
	}

	var retries int
	for retries < maxTransferRetries {
		err = c.attemptTransfer(fromKey, toKey, transferAmount)
		if err != nil {
			retries++
			var e *insufficientFundsError
			if errors.As(err, &e) {
				return fmt.Errorf("%s, aborting txn", err)
			}
			fmt.Printf("%s\nRetrying %d times...\n", err, retries)
		} else {
			color.HiGreen("XFER Successful")
			return nil
		}
	}

	return errors.New("Retries exhausted, aborting txn")
}

func (c *RaftKVClient) attemptTransfer(fromKey, toKey string, transferAmount int64) error {

	var fromValue int64
	var toValue int64

	/* Order of transactions to be sent to raft server. eg., transfer x y 5
	* 1. Fetch values for the fromKey, toKey in a single transaction.
	* 2. If successful, then send another transaction with the updated values for those keys.
		  set x server_fetched_value - 5
	      set y server_fetched_value + 5
	* 3. If successful, return back to the client with a success, fail for all other cases.
	*/
	c.txnCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.GET, Key: fromKey},
			{Method: common.GET, Key: toKey},
		},
		IsTxn: true,
	}

	// Send txn to the server & fetch the response
	getTxnRsp, err := c.Transaction()
	if err != nil {
		return fmt.Errorf("get txn failed with err: %s", err.Error())
	}

	for _, cmdRsp := range getTxnRsp.Commands {
		if cmdRsp.Key == fromKey {
			fromValue = cmdRsp.Value
		} else if cmdRsp.Key == toKey {
			toValue = cmdRsp.Value
		}
	}

	if fromValue < transferAmount {
		return fmt.Errorf("%w", &insufficientFundsError{key: fromKey, expect: transferAmount, remain: fromValue})
	}

	c.txnCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.SET, Key: fromKey, Value: fromValue - transferAmount, // new value
				Cond: &raftpb.Cond{
					Key:   fromKey,
					Value: fromValue, // old value
				},
			},
			{Method: common.SET, Key: toKey, Value: toValue + transferAmount, // new value
				Cond: &raftpb.Cond{
					Key:   toKey,
					Value: toValue, // old value
				},
			},
		},
		IsTxn: true,
	}

	_, err = c.Transaction()
	if err != nil {
		return fmt.Errorf("set txn failed with err: %s", err.Error())
	}

	return nil
}

func (c *RaftKVClient) Run() {
	for {
		cmdArr := c.readString()
		if err := c.validCmd(cmdArr); err != nil {
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
			val, _ := parseInt64(cmdArr[2])
			if err := c.Set(cmdArr[1], val); err != nil {
				fmt.Println(err)
			}
		case common.DEL:
			if err := c.Delete(cmdArr[1]); err != nil {
				fmt.Println(err)
			}
		case common.ADD, common.SUB:
			if err := c.AddTransaction(cmdArr); err != nil {
				fmt.Println(err)
			}
		case common.TXN:
			c.TransactionRun(cmdArr)
		case common.TRANSFER:
			if err := c.TransferTransaction(cmdArr); err != nil {
				fmt.Println(err)
			}
		case common.EXIT:
			fmt.Println("Stop client")
			os.Exit(0)
		}
	}
}

func (c *RaftKVClient) parseServerAddr(key string) (string, error) {
	u, err := url.Parse(c.serverAddr)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "key", key)
	return u.String(), nil
}

func (c *RaftKVClient) newRequest(method, key string, data []byte) (*http.Response, error) {
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

func (c *RaftKVClient) newTxnRequest(data []byte) (*http.Response, error) {
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

<<<<<<< HEAD
func (c *raftKVClient) redirectReqToLeader(key, method string, data []byte, maxRetries int) error {
	var resp *http.Response
	var err error

	if maxRetries > 0 {
		fmt.Printf("redirecting request to leader: %s\n", c.serverAddr)
		resp, err = c.newRequest(method, key, data)
		if err != nil {
			resp, err = c.retryReqExceptActive(method, key, data)
			return err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.New(string(body))
		} else if resp.StatusCode == http.StatusOK {
			color.HiGreen("OK")
			return nil
		} else if resp.StatusCode == http.StatusMisdirectedRequest {
			err = c.redirectReqToLeader(key, method, data, maxRetries-1)
		}
	} else {
		return errors.New("exhausted max leader redirections. retry...")
	}

	return err
}

// Service unavailable, retry with known servers
func (c *raftKVClient) retryReqExceptActive(method, key string,data []byte) (*http.Response, error) {
	var resp *http.Response
	var err error

	currActiveServer := c.serverAddr
	for _, value := range staticCoordServers {
		if value == currActiveServer {
			continue
		}
		c.serverAddr = value
		resp, err = c.newRequest(method, key, data)
		fmt.Printf("Retrying with alternate server:%s\n", c.serverAddr)
		if err != nil {
			fmt.Printf("err: %s\n", err)
			continue
		}
		return resp, nil // found a responsive node (not necessarily leader)
	}

	return nil, err
}

func (c *raftKVClient) Get(key string) error {
	var resp *http.Response
	var err error

	resp, err = c.newRequest(http.MethodGet, key, nil)
=======
func (c *RaftKVClient) Get(key string) error {
	resp, err := c.newRequest(http.MethodGet, key, nil)
>>>>>>> 6d4de027221f82b113d3412229a6e8457cc00d7f
	if err != nil {
		fmt.Printf("err: %s\n", err)
		resp, err = c.retryReqExceptActive(http.MethodGet, key, nil)
	}

	if err != nil { return err }
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		color.HiGreen(string(body))
		return nil
	} else if resp.StatusCode == http.StatusMisdirectedRequest {
		// Update leader so this request can be retried at the leader
		c.serverAddr = staticIPLeaderMapping[string(body)]
		return c.redirectReqToLeader(key, http.MethodGet, nil, maxTransferRetries)
	}
	return errors.New(string(body))
}

func (c *RaftKVClient) Set(key string, value int64) error {
	var reqBody []byte
	var err error
	if reqBody, err = proto.Marshal(&raftpb.Command{
		Method: common.SET,
		Key:    key,
		Value:  value,
	}); err != nil {
		return err
	}
	resp, err := c.newRequest(http.MethodPost, key, reqBody)
	if err != nil {
		fmt.Printf("err: %s\n", err)
		resp, err = c.retryReqExceptActive(http.MethodPost, key, reqBody)
	}
	if err != nil { return err }
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK {
		color.HiGreen("OK")
		return nil
	} else if resp.StatusCode == http.StatusMisdirectedRequest {
		c.serverAddr = staticIPLeaderMapping[string(body)]
		return c.redirectReqToLeader(key, http.MethodPost, reqBody, maxTransferRetries)
	}
	return errors.New(string(body))
}

func (c *RaftKVClient) Delete(key string) error {
	resp, err := c.newRequest(http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("err: %s\n", err)
		resp, err = c.retryReqExceptActive(http.MethodDelete, key, nil)
	}

	if err != nil { return err }

	if resp.StatusCode == http.StatusOK {
		color.HiGreen("OK")
		return nil
	} else if resp.StatusCode == http.StatusMisdirectedRequest {
		c.serverAddr = staticIPLeaderMapping[string(body)]
		return c.redirectReqToLeader(key, http.MethodDelete, nil, maxTransferRetries)
	}
	return errors.New(string(body))
}

func (c *RaftKVClient) OptimizeTxnCommands() {
	lastSetMap := make(map[string]int)
	txnSkips := make([]bool, len(c.txnCmds.Commands))
	/* lastSetMap contains only valid keys (no `del` cmd followed by `set` in input cmd seq).
	*  If Keys exist, they are mapped to the index of last "set cmd" in the input cmd seq.
	*
	*  Also, to guarantee same ordering of commands as the input in txn, maintain separate
	* array txnSkips to skip values containing `True`.
	 */
	for idx, cmd := range c.txnCmds.Commands {
		switch cmd.Method {
		case common.SET:
			val, ok := lastSetMap[cmd.Key]
			if ok {
				txnSkips[val] = true // skip
			}
			lastSetMap[cmd.Key] = idx
		case common.DEL:
			val, ok := lastSetMap[cmd.Key]
			if ok {
				txnSkips[val] = true // skip
				txnSkips[idx] = true // skip
				delete(lastSetMap, cmd.Key)
			}
		}
	}

	var newCmds []*raftpb.Command
	for idx, ifSkip := range txnSkips {
		if !ifSkip {
			newCmds = append(newCmds, c.txnCmds.Commands[idx])
		}
	}
	c.txnCmds.Commands = newCmds
}

func (c *RaftKVClient) AddTransaction(cmdArr []string ) error {
	amount, _ := parseInt64(cmdArr[2])
	if amount == 0 {
		return errors.New("Non-zero value expected")
	}
	if cmdArr[0] == common.SUB {
		amount = -amount
	}
	var retries int
	for retries < maxTransferRetries {
		if err := c.attemptAdd(cmdArr[1], amount); err != nil {
			retries++
			fmt.Printf("%s\nRetrying %d times...\n", err, retries)
		} else {
			if cmdArr[0] == common.ADD {
				color.HiGreen("ADD successful")
			} else {
				color.HiGreen("SUB successful")
			}
			return nil
		}
	}
	return errors.New("Retries exhausted, aborting")
}

func (c *RaftKVClient) attemptAdd(key string, amount int64) error {
	c.txnCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.GET, Key: key},
		},
		IsTxn: true,
	}

	// Send txn to the server & fetch the response
	getTxnRsp, err := c.Transaction()
	if err != nil {
		return fmt.Errorf("get txn failed with err: %s", err.Error())
	}
	var oldValue int64
	for _, cmdRsp := range getTxnRsp.Commands {
		if cmdRsp.Key == key {
			oldValue = cmdRsp.Value
		}
	}
	c.txnCmds = &raftpb.RaftCommand{
		Commands: []*raftpb.Command{
			{Method: common.SET, Key: key, Value: oldValue + amount, // new value
				Cond: &raftpb.Cond{Key: key, Value: oldValue},
			},
		},
		IsTxn: true,
	}
	_, err = c.Transaction()
	if err != nil {
		return fmt.Errorf("set txn failed with err: %s", err.Error())
	}

	return nil
}

<<<<<<< HEAD
func (c* raftKVClient) transactionRedirectReqToLeader(reqBody []byte, maxRetries int) (*raftpb.RaftCommand, error) {
	var resp *http.Response
	var err error

	if maxRetries > 0 {
		fmt.Printf("redirecting request to leader: %s\n", c.serverAddr)
		resp, err = c.newTxnRequest(reqBody)
		if err != nil {
			resp, err = c.transactionRetryReq(reqBody)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.New(string(body))
		}
		txnCmdRsp := &raftpb.RaftCommand{}
		if err = proto.Unmarshal(body, txnCmdRsp); err != nil {
			return nil, errors.New(string(body))
		}
		if resp.StatusCode == http.StatusOK {
			color.HiGreen("OK")
			return txnCmdRsp, nil
		} else if resp.StatusCode == http.StatusMisdirectedRequest {
			_, err = c.transactionRedirectReqToLeader(reqBody, maxRetries-1)
		}
	} else {
		return nil, errors.New("exhausted max leader redirections. retry...")
	}

	return nil, err
}

func (c *raftKVClient) transactionRetryReq(reqBody []byte) (*http.Response, error) {
	var resp *http.Response
	var err error

	currActiveServer := c.serverAddr
	for _, value := range staticCoordServers {
		if value == currActiveServer { continue }
		c.serverAddr = value
		fmt.Printf("Retrying with alternate server:%s\n", c.serverAddr)
		resp, err = c.newTxnRequest(reqBody)
		if err != nil {
			fmt.Printf("err:%s\n", err)
			continue
		}
		return resp, nil // found some response from a live server (maynt be leader)
	}

	return nil, err
}

func (c *raftKVClient) Transaction() (*raftpb.RaftCommand, error) {
=======
func (c *RaftKVClient) Transaction() (*raftpb.RaftCommand, error) {
>>>>>>> 6d4de027221f82b113d3412229a6e8457cc00d7f
	oldLen := len(c.txnCmds.Commands)
	c.OptimizeTxnCommands()
	newLen := len(c.txnCmds.Commands)
	if newLen == 0 {
		fmt.Println("txn takes no effect so not submitting to server")
		return nil, nil
	} else if newLen < oldLen {
		fmt.Printf("Optimized txn to: %v \n", c.txnCmds.Commands)
	}
	// Only call single shard handler if len == 1 and isTxn flag is False
	if newLen == 1 && !c.txnCmds.IsTxn {
		return nil, c.txnToSingleCmd()
	}
	// To ensure handled by transaction instead of single shard handler
	// All txnCmds should be handled by transaction if getting here
	c.txnCmds.IsTxn = true
	fmt.Printf("Submitting %v\n", c.txnCmds.Commands)
	var reqBody []byte
	var err error
	if reqBody, err = proto.Marshal(c.txnCmds); err != nil {
		return nil, err
	}
	resp, err := c.newTxnRequest(reqBody)
	if err != nil {
		fmt.Printf("err:%s\n", err)
		resp, err = c.transactionRetryReq(reqBody)
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	txnCmdRsp := &raftpb.RaftCommand{}
	if err = proto.Unmarshal(body, txnCmdRsp); err != nil {
		return nil, errors.New(string(body))
	}
	if resp.StatusCode == http.StatusOK {
		color.HiGreen("OK")
		return txnCmdRsp, nil
	} else if resp.StatusCode == http.StatusMisdirectedRequest {
		c.serverAddr = staticIPLeaderMapping[string(body)]
		return c.transactionRedirectReqToLeader(reqBody, maxTransferRetries)
	}
	return nil, errors.New(string(body))
}

func (c *RaftKVClient) txnToSingleCmd() error {
	cmd := c.txnCmds.Commands[0]
	switch cmd.Method {
	case common.DEL:
		return c.Delete(cmd.Key)
	case common.SET:
		return c.Set(cmd.Key, cmd.Value)
	default:
		return errors.New(("Not implemented"))
	}
}
