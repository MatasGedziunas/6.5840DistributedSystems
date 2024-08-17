package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu        sync.Mutex
	db        map[string]string
	callsDone map[string]string // Changed to int64 for operation IDs
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastOpId, exists := kv.callsDone[args.ClientId]
	if !exists || args.OperationId > lastOpId { // Changed condition
		kv.db[args.Key] = args.Value
		kv.callsDone[args.ClientId] = args.OperationId
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastOpId, exists := kv.callsDone[args.ClientId]
	if !exists || args.OperationId > lastOpId { // Changed condition
		kv.db[args.Key] += args.Value
		kv.callsDone[args.ClientId] = args.OperationId
	}
	reply.Value = kv.db[args.Key]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string]string)
	kv.callsDone = make(map[string]string) // Changed to int64
	return kv
}
