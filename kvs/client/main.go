package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/rpc"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient    *rpc.Client
	clientID     uint64
	currentTxid  uint64            // 0 means no active txn
	writeSet     map[string]string // For read-your-own-writes
	participants []*rpc.Client     // Servers involved in this txn
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{
		rpcClient:    rpcClient,
		clientID:     rand.Uint64(),
		currentTxid:  0,
		writeSet:     make(map[string]string),
		participants: nil,
	}
}

func (client *Client) Begin() {
	if client.currentTxid != 0 {
		log.Fatal("Begin() called but transaction already active")
	}

	client.currentTxid = rand.Uint64()
	if client.currentTxid == 0 { // Avoid 0
		client.currentTxid = 1
	}

	// Reset
	client.writeSet = make(map[string]string)
	client.participants = nil
}

func (client *Client) Get(key string) (string, bool) {
	if client.currentTxid == 0 {
		log.Fatal("Get() called but no transaction active")
	}

	// Check writeSet first
	if value, found := client.writeSet[key]; found {
		return value, true
	}

	// If not in writeSet, query server
	request := kvs.GetRequest{
		Key:      key,
		ClientID: client.clientID,
		Txid:     client.currentTxid,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if !response.Success { // Lock acquisition failed - abort
		client.Abort()
		return "", false
	}

	// Add server to participants
	client.addParticipant(client.rpcClient)

	return response.Value, true
}

func (client *Client) addParticipant(rpcClient *rpc.Client) {
	// Check if already in participants list
	for _, p := range client.participants {
		if p == rpcClient {
			return
		}
	}
	client.participants = append(client.participants, rpcClient)
}

func (client *Client) Abort() {
	if client.currentTxid == 0 {
		log.Fatal("Abort() called but no transaction active")
	}

	// Send abort to all participants
	for _, participant := range client.participants {
		request := kvs.AbortRequest{
			ClientID: client.clientID,
			Txid:     client.currentTxid,
		}
		response := kvs.AbortResponse{}
		err := participant.Call("KVService.Abort", &request, &response)
		if err != nil {
			log.Printf("Abort RPC failed: %v", err)
		}
	}

	// Reset
	client.currentTxid = 0
	client.writeSet = make(map[string]string)
	client.participants = nil
}

func (client *Client) Commit() bool {
	if client.currentTxid == 0 {
		log.Fatal("Commit() called but no transaction active")
	}

	// Send commit to all participants
	for _, participant := range client.participants {
		request := kvs.CommitRequest{
			ClientID: client.clientID,
			Txid:     client.currentTxid,
		}
		response := kvs.CommitResponse{}
		err := participant.Call("KVService.Commit", &request, &response)
		if err != nil {
			log.Printf("Commit RPC failed: %v", err)
			// In 2PC, need complex recovery here
			client.Abort()
			return false
		}

		if !response.Success { // Commit failed at this participant
			client.Abort()
			return false
		}
	}

	// Reset
	client.currentTxid = 0
	client.writeSet = make(map[string]string)
	client.participants = nil

	return true
}

func (client *Client) Put(key string, value string) bool {
	if client.currentTxid == 0 {
		log.Fatal("Put() called but no transaction active")
	}

	// Update writeSet immediately
	client.writeSet[key] = value

	// Send to server
	request := kvs.PutRequest{
		Key:      key,
		Value:    value,
		ClientID: client.clientID,
		Txid:     client.currentTxid,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if !response.Success {
		client.Abort() // Lock acquisition failed - abort
		return false
	}

	// Add server to participants
	client.addParticipant(client.rpcClient)

	return true
}

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			if op.IsRead {
				client.Get(key)
			} else {
				client.Put(key, value)
			}
			opsCompleted++
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	host := hosts[0]
	clientId := 0
	go func(clientId int) {
		workload := kvs.NewWorkload(*workload, *theta)
		runClient(clientId, host, &done, workload, resultsCh)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
