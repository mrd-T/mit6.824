package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824-golabs-2020/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	identifier int64
	seq        uint64
	leaderId   int
	// You will have to modify this struct.
}

func (ck *Clerk) GetSeq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
}
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.identifier = nrand()
	ck.seq = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, Seq: ck.GetSeq(), Identifier: ck.identifier}

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated {
			ck.leaderId += 1
			ck.leaderId %= len(ck.servers)
			continue
		}

		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
			continue
		case ErrKeyNotExist:
			return reply.Value
		}

		return reply.Value
	}
	// You will have to modify this function.
	// return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.GetSeq(), Identifier: ck.identifier}
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated {
			ck.leaderId += 1 //找leader
			ck.leaderId %= len(ck.servers)
			continue
		}

		switch reply.Err { //重新发送
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	// DPrintf("client %v: Put key %v value %v", ck.identifier, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	// DPrintf("client %v: Append key %v value %v", ck.identifier, key, value)
	ck.PutAppend(key, value, "Append")
}
