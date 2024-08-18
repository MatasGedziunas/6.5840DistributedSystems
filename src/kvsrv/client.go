package kvsrv

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	server   *labrpc.ClientEnd
	curId    int64
	clientId int64
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.curId = 1
	ck.clientId = int64(nrand())
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, OperationId: ck.getId(), ClientId: strconv.Itoa(int(ck.clientId))}
	reply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, OperationId: ck.getId(), ClientId: strconv.Itoa(int(ck.clientId))}
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	return reply.Value
}

func (ck *Clerk) getId() int {
	ck.mu.Lock()
	id := ck.curId
	ck.curId += 1
	ck.mu.Unlock()
	converted := int(id)
	return converted
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
