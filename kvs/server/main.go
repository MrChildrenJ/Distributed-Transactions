package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts    uint64
	gets    uint64
	commits uint64
	aborts  uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	r.commits = s.commits - prev.commits
	r.aborts = s.aborts - prev.aborts
	return r
}

type Transaction struct {
	ClientID uint64
	TxID     uint64
	ReadSet  map[string]bool
	WriteSet map[string]string
}

type KeyLock struct {
	Readers map[uint64]*Transaction // Map of TxID -> Transaction for shared locks
	Writer  *Transaction            // Exclusive lock holder (nil if no writer)
}

type KVService struct {
	sync.Mutex
	mp           map[string]string                  // Key-value store
	locks        map[string]*KeyLock                // Per-key lock state
	transactions map[uint64]*Transaction            // Active transactions by TxID
	stats        Stats
	prevStats    Stats
	lastPrint    time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]string)
	kvs.locks = make(map[string]*KeyLock)
	kvs.transactions = make(map[uint64]*Transaction)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) getOrCreateTransaction(clientID, txID uint64) *Transaction {
	if tx, exists := kv.transactions[txID]; exists {
		return tx
	}

	tx := &Transaction{
		ClientID: clientID,
		TxID:     txID,
		ReadSet:  make(map[string]bool),
		WriteSet: make(map[string]string),
	}
	kv.transactions[txID] = tx
	return tx
}

func (kv *KVService) tryAcquireReadLock(key string, tx *Transaction) bool {
	if keyLock, exists := kv.locks[key]; exists {
		// Check if there's a conflicting writer
		if keyLock.Writer != nil && keyLock.Writer.TxID != tx.TxID {
			return false // Writer conflict
		}
	} else {
		// No existing lock for this key
		kv.locks[key] = &KeyLock{
			Readers: make(map[uint64]*Transaction),
			Writer:  nil,
		}
	}

	// Add this transaction as a reader
	kv.locks[key].Readers[tx.TxID] = tx
	return true
}

func (kv *KVService) tryAcquireWriteLock(key string, tx *Transaction) bool {
	if keyLock, exists := kv.locks[key]; exists {
		// Check for any conflicting readers or writers
		if keyLock.Writer != nil && keyLock.Writer.TxID != tx.TxID {
			return false // Writer conflict
		}
		for readerTxID := range keyLock.Readers {
			if readerTxID != tx.TxID {
				return false // Reader conflict
			}
		}
	} else {
		// No existing lock for this key
		kv.locks[key] = &KeyLock{
			Readers: make(map[uint64]*Transaction),
			Writer:  nil,
		}
	}

	// Acquire exclusive write lock
	kv.locks[key].Writer = tx
	return true
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.gets++

	// Get or create transaction
	tx := kv.getOrCreateTransaction(request.ClientID, request.Txid)

	// Try to acquire read lock
	if !kv.tryAcquireReadLock(request.Key, tx) {
		response.Success = false
		return nil
	}

	// Add to read set
	tx.ReadSet[request.Key] = true

	// Check if key is in transaction's write set first
	if value, found := tx.WriteSet[request.Key]; found {
		response.Value = value
	} else if value, found := kv.mp[request.Key]; found {
		response.Value = value
	}

	response.Success = true
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.puts++

	// Get or create transaction
	tx := kv.getOrCreateTransaction(request.ClientID, request.Txid)

	// Try to acquire write lock
	if !kv.tryAcquireWriteLock(request.Key, tx) {
		response.Success = false
		return nil
	}

	// Add to write set (don't modify main store until commit)
	tx.WriteSet[request.Key] = request.Value

	response.Success = true
	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\ncommit/s %0.2f\nabort/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
