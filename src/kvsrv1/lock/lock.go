package lock

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	owner   string
	mu      sync.Mutex
	lockKey string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.ck = ck
	lk.lockKey = l // 锁的标识key
	return lk
}

func (lk *Lock) Acquire() {
	id := kvtest.RandValue(8)          // 客户端标识
	_, _, err := lk.ck.Get(lk.lockKey) // 检查锁是否存在
	if err == rpc.ErrNoKey {           // 不存在这个锁
		for {
			err := lk.ck.Put(lk.lockKey, "0", 0) // 新建一个 key 为 lk.lockKey, value 为 "0" 的锁，0表示空闲，非0表示被获取
			if err == rpc.ErrMaybe {
				continue
			}
			break
		}
	}
	for { // 等待获取锁
		value, version, _ := lk.ck.Get(lk.lockKey)
		if value == "0" { // 锁未被占用
			lk.mu.Lock()
			err := lk.ck.Put(lk.lockKey, id, version) // 占用锁
			// 由于这个put是否成功并不清楚，先把owner设置了，不要等到ok了再设置owner，不然可能会出现明明已经获取到了锁，但是ok丢失了，一直返回Errversion导致无限循环
			lk.owner = id
			if err == rpc.ErrMaybe || err == rpc.ErrVersion {
				lk.mu.Unlock()
				continue
			} else if err == rpc.OK { // 原子化操作，只有当客户端成功占用了锁才break，不要通过get和put两个操作来判断break条件
				lk.mu.Unlock()
				break
			}
			lk.mu.Unlock()
		} else if value == lk.owner { // 检查锁是否是自己的
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.lockKey) // 检查锁是否存在
	if value == lk.owner {
		for {
			err := lk.ck.Put(lk.lockKey, "0", version)
			if err == rpc.ErrMaybe {
				continue
			}
			break
		}
	}
}
