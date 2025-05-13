package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex   // 锁
	data map[string]v // 数据
	// Your definitions here.
}

type v struct {
	value   string       // 值
	version rpc.Tversion // 版本
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]v),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[args.Key]
	if !ok { // 不存在这个数据，报错
		reply.Err = rpc.ErrNoKey
	} else { // 返回查询结果
		reply.Value = v.value
		reply.Version = v.version
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Version == 0 { // 新数据
		_, ok := kv.data[args.Key]
		if !ok { // 不存在这个数据，则新建
			kv.data[args.Key] = v{
				value:   args.Value,
				version: args.Version + 1,
			}
			reply.Err = rpc.OK
		} else { // 数据已经存在，报错
			reply.Err = rpc.ErrVersion
		}
	} else { // 更新数据
		_, ok := kv.data[args.Key]
		if !ok { // 不存在这个数据，报错
			reply.Err = rpc.ErrNoKey
		} else { // 更新数据
			if kv.data[args.Key].version != args.Version {
				reply.Err = rpc.ErrVersion
			} else {
				kv.data[args.Key] = v{
					value:   args.Value,
					version: args.Version + 1,
				}
				reply.Err = rpc.OK
			}
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
