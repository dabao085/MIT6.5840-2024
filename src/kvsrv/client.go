package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	seq        uint64
	identifier int64 // identify Clerk
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
	// You'll have to add code here.
	ck.identifier = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) GetSeq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
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

	// You will have to modify this function.
	args := GetArgs{}

	args.Key = key
	args.Identifier = ck.identifier
	ok := false
	for {
		reply := GetReply{}
		ok = ck.server.Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		return reply.Value
	}
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
	args := PutAppendArgs{}

	args.Key = key
	args.Value = value
	args.Seq = ck.GetSeq()
	args.Identifier = ck.identifier

	ok := false
	for {
		reply := PutAppendReply{}
		ok = ck.server.Call("KVServer."+op, &args, &reply)
		if !ok {
			//log.Printf("Clerk %d retry: %s", ck.identifier, "KVServer." + op)
			continue
		}
		return reply.Value
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
