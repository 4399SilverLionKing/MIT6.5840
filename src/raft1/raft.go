package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex            // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd   // RPC end points of all peers
	persister   *tester.Persister     // Object to hold this peer's persisted state
	me          int                   // this peer's index into peers[]
	dead        int32                 // set by Kill()
	votedFor    int                   // 投票给谁了
	heartbeat   int                   // 收到谁的心跳
	logs        []LogEntry            // 日志列表
	state       int                   // 当前状态，0 表示 flower，1 表示 candidate，2 表示 leader
	currentTerm int                   // 当前任期
	commitIndex int                   // 所有服务器上已知已提交的最高日志条目的索引
	lastApplied int                   // 当前服务器已应用到其状态机的最高日志条目的索引
	nextIndex   []int                 // leader 认为应该发送给服务器的下一个日志条目的索引
	matchIndex  []int                 // 已匹配的最高日志条目索引数组
	applyCh     chan raftapi.ApplyMsg // 用于向上层发送已提交的日志
}

type LogEntry struct {
	Arg   Argment // 操作命令
	Term  int     // 任期号
	Index int     // 日志索引
}

type Argment struct {
	Command interface{}
}

// InstallSnapshot RPC参数结构体
type InstallSnapshotArgs struct {
	Term              int    // leader的任期
	LeaderId          int    // leader的ID
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期
	Data              []byte // 快照数据
}

// InstallSnapshot RPC响应结构体
type InstallSnapshotReply struct {
	Term int // follower的当前任期，用于leader更新自己
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm // 当前任期
	if rf.state == 2 {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 按照Raft论文Figure 2，只需持久化以下状态
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	raftstate := w.Bytes()
	// 使用当前的快照进行持久化
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Panic("Raft读取持久化数据失败")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查索引是否有效
	if index <= 0 || index >= len(rf.logs) {
		return // 无效的索引，忽略此快照请求
	}

	// 如果当前日志已经被压缩到更新的位置，忽略旧的快照
	if rf.logs[0].Index >= index {
		return
	}

	// 找到数组索引与日志索引的映射关系
	// 将日志索引转换为数组索引
	arrayIndex := 0
	for i, entry := range rf.logs {
		if entry.Index == index {
			arrayIndex = i
			break
		}
	}

	// 确保找到了有效的数组索引
	if arrayIndex == 0 && index != rf.logs[0].Index {
		// 没有找到对应的数组索引，这可能是一个错误
		return
	}

	// 创建新的持久化状态
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 获取快照中最后日志条目的任期
	lastIncludedTerm := rf.logs[arrayIndex].Term

	// 保留index之后的日志
	newLogs := make([]LogEntry, 0)
	// 创建一个空日志条目作为哨兵，用来存储快照的最后索引和任期
	newLogs = append(newLogs, LogEntry{
		Term:  lastIncludedTerm,
		Index: index,
		Arg:   Argment{}, // 显式初始化Arg字段
	})

	// 将index之后的日志追加到新日志中（保留原始索引）
	for i := arrayIndex + 1; i < len(rf.logs); i++ {
		newLogs = append(newLogs, rf.logs[i])
	}

	// 更新日志
	rf.logs = newLogs

	// 编码持久化状态
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	// 保存状态和快照
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	// 更新commitIndex和lastApplied，如果需要的话
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
}

// InstallSnapshot RPC处理函数
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 返回当前任期
	reply.Term = rf.currentTerm

	// 如果term < currentTerm就立即返回
	if args.Term < rf.currentTerm {
		return
	}

	// 如果发现更高的term，转变为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0 // 变回follower
		rf.votedFor = -1
		rf.persist()
	}

	// 收到心跳
	rf.heartbeat = args.LeaderId

	// 如果快照包含的最后索引小于等于已提交的索引，或者我们已经有包含该索引的日志，
	// 说明这是一个过时的快照，可以忽略
	if args.LastIncludedIndex <= rf.commitIndex {
		// 已经提交的日志不能被旧的快照覆盖
		return
	}

	// 创建新的日志数组，从快照点开始
	newLogs := make([]LogEntry, 0)
	// 添加哨兵日志条目，保存快照的最后日志信息
	newLogs = append(newLogs, LogEntry{
		Term:  args.LastIncludedTerm,
		Index: args.LastIncludedIndex,
		Arg:   Argment{}, // 显式初始化Arg字段
	})

	// 检查现有日志是否包含快照的最后索引
	logContainsSnapshot := false
	snapshotArrayIndex := 0

	for i, entry := range rf.logs {
		if entry.Index == args.LastIncludedIndex {
			logContainsSnapshot = true
			snapshotArrayIndex = i
			break
		}
	}

	// 如果现有日志与快照有重叠，并且任期匹配，保留快照之后的日志
	if logContainsSnapshot && rf.logs[snapshotArrayIndex].Term == args.LastIncludedTerm {
		// 保留快照之后的日志
		for i := snapshotArrayIndex + 1; i < len(rf.logs); i++ {
			newLogs = append(newLogs, rf.logs[i])
		}
	} else {
		// 如果没有重叠或任期不匹配，丢弃所有现有日志
		// 新的日志只包含哨兵条目
	}

	// 更新日志
	rf.logs = newLogs

	// 更新提交索引和应用索引
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	// 持久化状态和快照
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	// 通知上层应用安装快照（直接在当前goroutine中发送，避免在并发情况下的状态不一致）
	// 但我们不应该在持有锁的情况下发送到channel，因为可能导致死锁
	snapshot := args.Data
	snapshotTerm := args.LastIncludedTerm
	snapshotIndex := args.LastIncludedIndex

	// 在解锁后发送消息
	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  snapshotTerm,
			SnapshotIndex: snapshotIndex,
		}
	}()
}

// 发送InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 选举者当前任期
	CandidateId  int // 选举者编号
	LastLogIndex int // 选举者最后一个log的序号
	LastLogTerm  int // 选举者最后一个log的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term    int  // 该peer当前的任期
	IsVoted bool // 是否投票了，false表示不投票，true表示投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 默认不投票
	reply.Term = rf.currentTerm
	reply.IsVoted = false

	// 如果请求的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 如果发现更高的任期，转变为跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0 // 变回follower
		rf.votedFor = -1
		rf.heartbeat = -1
		// 任期变更，需要持久化
		rf.persist()
	}

	// 检查是否已经投票给其他候选人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 检查候选人的日志是否至少和自己一样新
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = rf.logs[lastLogIndex].Term
		}

		// 根据Raft论文，日志比较规则：
		// 1. 最后一个日志条目的任期号更大的更新
		// 2. 如果任期号相同，则日志更长的更新
		logUpToDate := false
		if args.LastLogTerm > lastLogTerm {
			logUpToDate = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			logUpToDate = true
		}

		if logUpToDate {
			// 投票给候选人
			rf.votedFor = args.CandidateId
			reply.IsVoted = true
			rf.heartbeat = args.CandidateId // 重置心跳计时器
			// 投票结果变更，需要持久化
			rf.persist()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct { // 发送日志参数
	Term         int        // leader 当前任期
	LeaderId     int        // leader 编号
	PrevLogIndex int        // 最新日志的序号
	PrevLogTerm  int        // 最新日志所在任期
	Entries      []LogEntry // 需要发送的日志
	LeaderCommit int        // leader 已提交的最高日志条目的索引
}

type AppendEntriesReply struct { // 响应日志参数
	Term          int  // 响应者当前任期
	Success       bool // 是否成功记录
	ConflictTerm  int  // 冲突的任期号
	ConflictIndex int  // 冲突任期的第一个索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeat = args.LeaderId // 收到心跳

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 更新任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0 // 变回follower
		rf.votedFor = -1
		// 任期变更，需要持久化
		rf.persist()
	}

	// 2. 日志一致性检查
	if args.PrevLogIndex > 0 {
		if len(rf.logs) <= args.PrevLogIndex {
			// 日志不够长，返回当前日志长度作为冲突索引
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1
			return
		}

		if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			// 任期不匹配，返回冲突任期和该任期的第一个索引
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

			// 找到该任期的第一个日志条目
			conflictIndex := args.PrevLogIndex
			for i := args.PrevLogIndex - 1; i > 0; i-- {
				if rf.logs[i].Term != reply.ConflictTerm {
					break
				}
				conflictIndex = i
			}
			reply.ConflictIndex = conflictIndex
			return
		}
	}

	// 3. 处理新日志
	logChanged := false
	if args.Entries != nil {
		// 删除冲突的日志
		nextIdx := args.PrevLogIndex + 1
		for i, entry := range args.Entries {
			if nextIdx+i < len(rf.logs) {
				if rf.logs[nextIdx+i].Term != entry.Term {
					// 删除从这里开始的所有日志
					rf.logs = rf.logs[:nextIdx+i]
					logChanged = true
					break
				}
			} else {
				break
			}
		}

		// 添加新日志
		if nextIdx+len(args.Entries) > len(rf.logs) {
			// 只添加不存在的部分
			startAppend := max(nextIdx, len(rf.logs))
			appendEntries := args.Entries[startAppend-nextIdx:]
			rf.logs = append(rf.logs, appendEntries...)
			logChanged = true
		}
	}

	// 如果日志有变化，进行持久化
	if logChanged {
		rf.persist()
	}

	// 4. 更新提交索引
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != 2 { // 不是leader
		return -1, -1, false
	}

	index := len(rf.logs)
	term := rf.currentTerm

	// 添加日志到自己的日志中
	entry := LogEntry{
		Arg:   Argment{Command: command},
		Term:  term,
		Index: index,
	}
	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = index // 更新自己的matchIndex

	// 添加新日志后立即持久化
	rf.persist()

	// 立即发送一轮心跳/日志复制
	go rf.broadcastHeartbeat()

	return index, term, true
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != 2 { // 不是leader则退出
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			// 增加重试次数上限和退避策略
			retries := 0
			maxRetries := 5 // 最大重试次数
			baseBackoff := 10 * time.Millisecond
			maxBackoff := 200 * time.Millisecond

			for retries < maxRetries && !rf.killed() {
				rf.mu.Lock()
				if rf.state != 2 || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}

				// 检查是否需要发送快照而不是普通的AppendEntries
				if len(rf.logs) > 0 && rf.nextIndex[server] <= rf.logs[0].Index {
					// 需要发送快照
					snapArgs := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.logs[0].Index,
						LastIncludedTerm:  rf.logs[0].Term,
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()

					snapReply := &InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, snapArgs, snapReply)

					if ok {
						rf.mu.Lock()
						if snapReply.Term > rf.currentTerm {
							// 发现更高任期，变回follower
							rf.currentTerm = snapReply.Term
							rf.state = 0
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}

						// 快照安装成功，更新nextIndex和matchIndex
						rf.nextIndex[server] = snapArgs.LastIncludedIndex + 1
						rf.matchIndex[server] = snapArgs.LastIncludedIndex
						rf.mu.Unlock()
						return
					}
					// 如果快照安装失败，继续重试
				} else {
					// 计算prevIndex，确保不会越界
					prevIndex := rf.nextIndex[server] - 1
					prevTerm := 0

					// 找到prevIndex对应的数组索引
					arrayIndex := -1
					for i, entry := range rf.logs {
						if entry.Index == prevIndex {
							arrayIndex = i
							break
						}
					}

					// 如果找到了对应的数组索引，获取其任期
					if arrayIndex >= 0 {
						prevTerm = rf.logs[arrayIndex].Term
					} else if len(rf.logs) > 0 {
						// 如果没有找到，但prevIndex小于第一个日志条目的索引
						// 这意味着prevIndex被快照包含了，我们应该发送快照
						if prevIndex <= rf.logs[0].Index {
							rf.nextIndex[server] = rf.logs[0].Index
							rf.mu.Unlock()
							continue // 重试，这次会发送快照
						}
					}

					// 从nextIndex开始的所有日志条目
					var entries []LogEntry
					startIdx := -1
					for i, entry := range rf.logs {
						if entry.Index == rf.nextIndex[server] {
							startIdx = i
							break
						}
					}

					if startIdx >= 0 {
						entries = rf.logs[startIdx:]
					}

					args := &AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevIndex,
						PrevLogTerm:  prevTerm,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)

					if ok {
						rf.mu.Lock()

						// 任期检查
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = 0 // 变回follower
							rf.votedFor = -1
							rf.persist() // 持久化状态变更
							rf.mu.Unlock()
							return
						}

						if reply.Success {
							if len(entries) > 0 {
								// 更新nextIndex和matchIndex
								newNextIndex := prevIndex + 1 + len(entries)
								rf.nextIndex[server] = newNextIndex
								rf.matchIndex[server] = newNextIndex - 1

								// 尝试更新commitIndex
								for n := rf.commitIndex + 1; n < len(rf.logs); n++ {
									if rf.logs[n].Term == rf.currentTerm {
										count := 1 // 领导者自己
										for j := 0; j < len(rf.peers); j++ {
											if j != rf.me && rf.matchIndex[j] >= n {
												count++
											}
										}
										if count > len(rf.peers)/2 {
											rf.commitIndex = n
										}
									}
								}
							}
							rf.mu.Unlock()
							return // 成功就返回
						} else {
							// 日志一致性失败处理
							if rf.nextIndex[server] > 1 {
								// 如果收到了冲突信息
								if reply.ConflictTerm != -1 {
									// 尝试找到我们日志中与冲突任期相同的最后一个日志条目
									lastIndex := -1
									for i := len(rf.logs) - 1; i > 0; i-- {
										if rf.logs[i].Term == reply.ConflictTerm {
											lastIndex = i
											break
										}
									}

									if lastIndex != -1 {
										// 找到了相同任期的日志，将nextIndex设置为该任期的最后一个日志的下一个位置
										rf.nextIndex[server] = lastIndex + 1
									} else {
										// 没找到相同任期的日志，直接跳到冲突索引处
										rf.nextIndex[server] = reply.ConflictIndex
									}
								} else {
									// 如果没有冲突任期信息，直接使用冲突索引
									rf.nextIndex[server] = reply.ConflictIndex
								}

								// 确保nextIndex至少为1
								rf.nextIndex[server] = max(1, rf.nextIndex[server])
							}
							rf.mu.Unlock()

							// 实现指数退避策略
							retries++
							backoff := time.Duration(min(
								int64(maxBackoff),
								int64(baseBackoff)*(1<<uint(retries)),
							))
							time.Sleep(backoff)
							continue // 继续尝试
						}
					} else {
						// RPC调用失败，实现指数退避策略
						retries++
						backoff := time.Duration(min(
							int64(maxBackoff),
							int64(baseBackoff)*(1<<uint(retries)),
						))
						time.Sleep(backoff)
						continue // 继续尝试
					}
				}
			}
		}(i)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	heartbeatInterval := 100 * time.Millisecond // 更短的心跳间隔

	for !rf.killed() {
		electionTimeout := 300 + rand.Int63()%300 // 300-600ms的选举超时

		// 1. 检查是否需要开始选举
		rf.mu.Lock()
		isHeartbeatTimeout := (rf.heartbeat == -1) && (rf.state != 2) // 不是领导者且未收到心跳
		rf.mu.Unlock()

		if isHeartbeatTimeout {
			rf.mu.Lock()
			// 变成候选人
			rf.state = 1
			rf.currentTerm++    // 增加任期
			rf.votedFor = rf.me // 给自己投票

			// 状态变更，需要持久化
			rf.persist()

			// 准备选举参数
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.logs[lastLogIndex].Term
			}

			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			// 释放锁，准备发送RPC
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			// 使用原子计数跟踪选票
			var voteCount atomic.Int32
			voteCount.Add(1) // 自己投给自己

			// 向所有其他服务器请求投票
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				go func(server int) {
					reply := &RequestVoteReply{}
					if rf.sendRequestVote(server, args, reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						// 如果发现更高任期，转变为跟随者
						if reply.Term > currentTerm {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = 0 // 变为跟随者
								rf.votedFor = -1
								// 状态变更，需要持久化
								rf.persist()
							}
							return
						}

						// 如果获得投票且仍在当前选举中
						if reply.IsVoted && rf.state == 1 && rf.currentTerm == currentTerm {
							// 增加选票计数
							if voteCount.Add(1) > int32(len(rf.peers)/2) {
								// 赢得选举，成为领导者
								if rf.state == 1 && rf.currentTerm == currentTerm {
									rf.state = 2 // 成为领导者

									// 初始化领导者状态
									for j := 0; j < len(rf.peers); j++ {
										rf.nextIndex[j] = len(rf.logs)
										rf.matchIndex[j] = 0
									}

									// 状态变更为领导者，需要持久化
									rf.persist()

									// 成为领导者后立即发送心跳
									go rf.broadcastHeartbeat()
								}
							}
						}
					}
				}(i)
			}
		}

		// 2. 如果是领导者，发送心跳
		rf.mu.Lock()
		isLeader := (rf.state == 2)
		rf.mu.Unlock()

		if isLeader {
			rf.broadcastHeartbeat()
			time.Sleep(heartbeatInterval)
		} else {
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
			rf.mu.Lock()
			rf.heartbeat = -1 // 重置心跳状态，准备下一次检查
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier(applyCh chan<- raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && rf.commitIndex < len(rf.logs) {
			// 有新提交的日志需要应用
			rf.lastApplied++

			// 找到lastApplied对应的数组索引
			applyIndex := 0
			for i, entry := range rf.logs {
				if entry.Index == rf.lastApplied {
					applyIndex = i
					break
				}
			}

			// 确保找到了有效的索引
			if applyIndex > 0 || (applyIndex == 0 && rf.logs[0].Index == rf.lastApplied) {
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[applyIndex].Arg.Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				applyCh <- applyMsg
			} else {
				// 如果找不到对应的日志条目，可能是因为已经被快照替代了
				// 这种情况下我们不应该应用任何命令
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// 初始化基本状态
	rf.votedFor = -1  // 没有投票
	rf.heartbeat = -1 // 没有心跳
	rf.state = 0      // 初始为follower
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0

	// 初始化日志，索引0位置使用特殊值
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0, Index: 0, Arg: Argment{Command: nil}} // 明确初始化第一个日志条目

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// 从持久化存储恢复状态
	rf.readPersist(persister.ReadRaftState())

	// 开始选举和应用日志的协程
	go rf.ticker()
	go rf.applier(applyCh)

	return rf
}
