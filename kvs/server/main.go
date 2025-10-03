package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
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

type Operation struct {
	OpType string // "GET" or "PUT"
	Key    string
	Value  string // empty for GET operations
}

// tracks lock holders for a specific "key"
type LockInfo struct {
	readHolders map[uint64]bool // Set of transactions holding read locks
	writeHolder *uint64         // Transaction holding write lock (nil if none)
}

func NewLockInfo() *LockInfo {
	return &LockInfo{
		readHolders: make(map[uint64]bool),
		writeHolder: nil,
	}
}

type KVService struct {
	sync.RWMutex          // embedded, KVService "inherits" all methods of sync.RWMutex (even though go has no inheritence feature)
	mp           sync.Map // map[string]string - actual key-value store
	locks        sync.Map // map[string]*LockInfo - lock management; 追蹤每個 key 被哪些 transaction 鎖定
	transactions sync.Map // map[uint64][]Operation - transaction operations	txn id: ops[]
	stats        Stats
	prevStats    Stats
	lastPrint    time.Time
}

func NewKVService() *KVService {
	// Don't need to initialize sync.Map vars because they are available empty map; zero value of RWMutex is also available un-lock state
	// Stats is consist of uint64 whose zero value is zero
	kvs := &KVService{} // kvs is a pointer
	kvs.lastPrint = time.Now()
	return kvs // return pointer
}

// acquireReadLock attempts to acquire a read lock for the given transaction on the given key
func (kv *KVService) acquireReadLock(key string, txid uint64) error {
	lockInfoVal, _ := kv.locks.LoadOrStore(key, NewLockInfo()) // return value(type: any), loaded(bool)
	// sync.Map is a generic container (an implementation from before generics were introduced) -> 取出時，編譯器不知道具體類型，只知道是 any
	lockInfo := lockInfoVal.(*LockInfo) // I know lockInfoVal contains *LockInfo, convert to this type. If assertion failed（wrong type), get panic

	// Check if there's a write lock held by a different transaction
	if lockInfo.writeHolder != nil && *lockInfo.writeHolder != txid {
		return errors.New("Cannot acquire Read Lock, key is currently write locked") // returns an error that formats as the given text.
	}

	// If this transaction already holds the write lock, it can also read
	if lockInfo.writeHolder != nil && *lockInfo.writeHolder == txid {
		return nil // Already has write lock, which includes read access
	}

	// No other transactions on the given key have lock
	lockInfo.readHolders[txid] = true
	return nil
}

// acquireWriteLock attempts to acquire a write lock for the given transaction on the given key
func (kv *KVService) acquireWriteLock(key string, txid uint64) error {
	lockInfoVal, _ := kv.locks.LoadOrStore(key, NewLockInfo())
	lockInfo := lockInfoVal.(*LockInfo)

	// If this transaction already holds the write lock, allow it
	if lockInfo.writeHolder != nil && *lockInfo.writeHolder == txid {
		return nil
	}

	// If another transaction holds the write lock, deny
	if lockInfo.writeHolder != nil {
		return errors.New("Cannot acquire Write Lock, key is currently write locked")
	}

	// If there are read locks held by other transactions, deny
	// Exception: if only this transaction holds a read lock, allow upgrade
	if len(lockInfo.readHolders) > 1 {
		return errors.New("Cannot acquire Write Lock, key has multiple read locks")
	}
	if len(lockInfo.readHolders) == 1 && !lockInfo.readHolders[txid] {
		return errors.New("Cannot acquire Write Lock, key is read locked by another transaction")
	}

	// Acquire write lock
	lockInfo.writeHolder = &txid
	// Remove from read holders if it was there (lock upgrade case)
	delete(lockInfo.readHolders, txid)

	return nil
}

func (kv *KVService) releaseLocks(txid uint64) {
	// Get all operations for this transaction to find which keys to unlock
	if ops, found := kv.transactions.Load(txid); found { // return interface
		if operations, ok := ops.([]Operation); ok { // convert ops to []Operations
			for _, op := range operations {
				if lockInfoVal, found := kv.locks.Load(op.Key); found { // return interface
					lockInfo := lockInfoVal.(*LockInfo) // convert lockInfoVal to *LockInfo

					// remove share locks associated with the txid
					delete(lockInfo.readHolders, txid) // func delete(m map[Type]Type1, key Type)

					// remove exclusive lock
					if lockInfo.writeHolder != nil && *lockInfo.writeHolder == txid {
						lockInfo.writeHolder = nil
					}
				}
			}
		}
	}

	// Purposes:
	// 	1. Prevent memory leaks
	// 	2. Maintain data consistency
	// When should we call releaseLocks():
	//	a. Commit
	//	b. Abort
	//	c. Encountering errors during Get/Put
	// In these situations, we all need to clean txid from transactions map
	kv.transactions.Delete(txid)
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.Lock()
	defer kv.Unlock()

	if _, found := kv.transactions.Load(request.Txid); !found {
		kv.transactions.Store(request.Txid, make([]Operation, 0, 4))
	}

	err := kv.acquireReadLock(request.Key, request.Txid)
	if err != nil { // acquireReadLock return error only if another transaction holds "write lock" (Read-write conflict)
		kv.releaseLocks(request.Txid)         // realease ALL locks associated with this txn since we are gonna abort
		atomic.AddUint64(&kv.stats.aborts, 1) // aborts++
		return err
	}

	// Add operation to transaction log. These ops must be atomic!
	ops, _ := kv.transactions.Load(request.Txid)
	operations := ops.([]Operation)
	operations = append(operations, Operation{
		OpType: "GET",
		Key:    request.Key,
	})
	kv.transactions.Store(request.Txid, operations)
	// Above ops must be atomic!

	// Read the value from up right away
	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
	} else { // Key doesn't exist
		response.Value = ""
	}

	atomic.AddUint64(&kv.stats.gets, 1)
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.Lock()
	defer kv.Unlock()

	if _, found := kv.transactions.Load(request.Txid); !found {
		kv.transactions.Store(request.Txid, make([]Operation, 0, 4))
	}

	err := kv.acquireWriteLock(request.Key, request.Txid)
	if err != nil { // Write-write conflict
		kv.releaseLocks(request.Txid)
		atomic.AddUint64(&kv.stats.aborts, 1)
		return err
	}

	// Add operation to transaction log (buffered, will be applied on commit)
	ops, _ := kv.transactions.Load(request.Txid)
	operations := ops.([]Operation)
	operations = append(operations, Operation{
		OpType: "PUT",
		Key:    request.Key,
		Value:  request.Value,
	})
	kv.transactions.Store(request.Txid, operations)
	// No modification on mp yet -> No kv.mp.Store()

	atomic.AddUint64(&kv.stats.puts, 1)
	return nil
}

// Commit applies all PUT operations from the transaction, then releases locks
func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()

	if operations, found := kv.transactions.Load(request.Txid); found {
		if ops, ok := operations.([]Operation); ok {
			for _, op := range ops { // Apply all PUT operations
				if op.OpType == "PUT" {
					kv.mp.Store(op.Key, op.Value)
				}
			}

			if request.Lead { // Only count commits for the lead participant
				atomic.AddUint64(&kv.stats.commits, 1)
			}
		}
	}

	// Release all locks held by this transaction
	kv.releaseLocks(request.Txid)
	return nil
}

// Abort discards all operations and releases locks
func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	// kv.Lock()
	// defer kv.Unlock()

	kv.releaseLocks(request.Txid)
	atomic.AddUint64(&kv.stats.aborts, 1)
	return nil
}

func (kv *KVService) printStats() {
	kv.RLock() // Read lock
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.RUnlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\ncommit/s %0.2f\nabort/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS,
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on") // return *string(pointer)
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	/* Regester functions with signature: func (t *T) MethodName(args *ArgsType, reply *ReplyType) error

	   So, in this project, this will "register" these methods:
		KVService.Get(request *kvs.GetRequest, response *kvs.GetResponse) error
		KVService.Put(request *kvs.PutRequest, response *kvs.PutResponse) error
		KVService.Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error
		KVService.Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error

		How to call on client?
		err := client.rpcClient.Call("KVService.Get", &request, &response)
	                             	 ^^^^^^^^^^^^^^^
	                                 ServiceName.MethodName
	*/
	rpc.HandleHTTP()
	/*
		將 RPC 服務註冊到 HTTP 路徑
		預設路徑：/_goRPC_
		允許通過 HTTP 傳輸 RPC 請求
	*/

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
