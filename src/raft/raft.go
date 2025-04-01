package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824-golabs-2020/labgob"
	"6.824-golabs-2020/labrpc"
)

// import "bytes"
// import "6.824-golabs-2020/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

var HeartBeatTimeout = 100 * time.Millisecond

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //Raft节点已终止
	Expire                  //投票(消息\竞选者）过期
	Voted                   //本Term内已经投过票

)

const (
	AppNormal    AppendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppRepeat                              // 追加重复 (2B
	AppCommitted                           // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B

)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	condApply *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status                  string     //状态
	term                    int        //第几轮投票
	voteFor                 int        //为谁投票,-1表示还没投票
	voteCount               int        //获得总票数,初始为0
	logs                    []LogEntry //日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
	commitIndex             int        // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied             int        // 最后一个被追加到状态机日志的索引值
	LastHeartBeatTime       time.Time
	LastElectionTimeoutTime time.Time
	ElectionTimeoutPeriod   int
	// leader拥有的可见变量，用来管理他的follower(leader经常修改的）
	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex      []int         // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex     []int         // 对于每一个server，已经复制给该server的最后日志条目下标
	applyChan      chan ApplyMsg // 日志都是存在这里client取（2B）
	overtime       time.Duration //任期倒计时总长
	heartbeatTimer *time.Timer   //心跳倒计时
	electionTimer  *time.Timer   //实现倒计时功能
}
type LogEntry struct {
	Term    int
	Command interface{}
}
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int                // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool               //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppState    AppendEntriesState // 追加状态
	UpNextIndex int                //下一个更新的点
	XTerm       int                // Follower中与Leader冲突的Log对应的Term
	XIndex      int                // Follower中，对应Term为XTerm的第一条Log条目的索引
	XLen        int                // Follower的log的长度
}

// 心跳时间重置
func (rf *Raft) ResetHeart() {
	rf.LastHeartBeatTime = time.Now()
}

// 判断心跳啊
func (rf *Raft) HeartBeatIsTimeout() bool {
	return time.Now().After(rf.LastHeartBeatTime.Add(HeartBeatTimeout))
}

// 选举重置
func (rf *Raft) ResetElectionTimer() {
	rf.ElectionTimeoutPeriod = 300 + rand.Int()%150
	rf.LastElectionTimeoutTime = time.Now()
}

// 选举超时
func (rf *Raft) ElectionIsTimeout() bool {
	if rf.status == "leader" {
		return false
	}
	return time.Now().After(rf.LastElectionTimeoutTime.Add(time.Millisecond * time.Duration(rf.ElectionTimeoutPeriod)))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = rf.status == "leader"
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// 初始化一个字节缓冲区用于序列化数据
	w := new(bytes.Buffer)
	// 创建一个labgob编码器，用于将Go对象编码为字节流
	e := labgob.NewEncoder(w)
	// 需要保存的内容，使用编码器将关键状态信息序列化到缓冲区
	// 如果编码过程中出现错误，则通过log.Fatal终止程序，防止数据损坏或不完整状态的保存
	if e.Encode(rf.term) != nil || e.Encode(rf.voteFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.voteCount) != nil {
		log.Fatal("Errors occur when encode the data!")
	}
	// 序列化完成后，从缓冲区获取字节切片准备存储
	data := w.Bytes()
	// 调用persister的SaveRaftState方法，将序列化后的数据保存到稳定的存储介质中
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		// 如果没有数据或数据长度不足，可能是初次启动，无需恢复状态
		return
	}
	// 初始化字节缓冲区以读取数据
	r := bytes.NewBuffer(data)
	// 创建一个labgob解码器，用于将字节流转换回Go对象
	d := labgob.NewDecoder(r)

	// 定义变量以接收解码后的状态信息
	var currentTerm int
	var votedFor int
	var votedCount int
	var logs []LogEntry

	// 按编码的顺序使用解码器从字节流中读取并还原状态信息
	// 如果解码过程中出现错误，通过log.Fatal终止程序，防止使用损坏或不完整的状态数据
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&votedCount) != nil {
		log.Fatal("Errors occur when decode the data!")
	} else {
		// 解码成功后，将读取的状态信息赋值给Raft实例的对应字段
		rf.term = currentTerm
		rf.voteFor = votedFor
		rf.logs = logs
		// rf.voteCount = votedCount
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后的日志索引
	LastLogTerm  int // 候选人最后的日志任期

}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票人的当前任期
	VoteGranted bool // true表示该节点把票投给了候选人
}

// 重置选举倒计时
func (rf *Raft) electionTimerreset() {
	rf.electionTimer.Stop()
	rf.overtime = time.Duration(150+rand.Intn(150)) * time.Millisecond
	// rf.electionTimer = time.NewTimer(rf.overtime)
	rf.electionTimer.Reset(rf.overtime)
}

// 重置心跳倒计时
func (rf *Raft) HeartBeatTimerreset() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

// 获得master的信息 没问题
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("args:%+v %d任务 from %d号任务(term:%d)\n", args, rf.me, args.LeaderId, args.Term)
	// Your code here (2A, 2B).
	// fmt.Printf("收到心跳!	%d号任务(term:%d)收到来自%d号任务(term:%d)的心跳!,\n", rf.me, rf.term, args.LeaderId, args.Term)
	//情况1: 收到的rpc的term太旧
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		reply.AppState = AppOutOfDate
		return

	}
	//情况2: 收到的rpc的term 比自己的term大或相等
	if args.Term > rf.term {
		rf.term = args.Term
		rf.status = "follower"
		rf.voteFor = -1
		rf.voteCount = 0
		rf.persist()
	}
	// rf.status = "follower"
	rf.ResetElectionTimer()
	// rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	// rf.electionTimer.Reset(rf.overtime)
	//情况3: 心跳日志不匹配
	isConflict := false

	// 校验PrevLogIndex和PrevLogTerm不合法
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.logs) {
		// PrevLogIndex位置不存在日志项
		reply.XTerm = -1
		reply.XLen = len(rf.logs) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置不存在日志项, Log长度为%v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// PrevLogIndex位置的日志项存在, 但term不匹配
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.logs[i].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = len(rf.logs) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.term
		reply.Success = false
		rf.persist()
		return
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
	// 	// 发生了冲突, 移除冲突位置开始后面所有的内容
	// 	DPrintf("server %v 的log与args发生冲突, 进行移除\n", rf.me)
	// 	rf.log = rf.log[:args.PrevLogIndex+1]
	// }
	for idx, log := range args.Entries {
		ridx := args.PrevLogIndex + 1 + idx
		if ridx < len(rf.logs) && rf.logs[ridx].Term != log.Term {
			// 某位置发生了冲突, 覆盖这个位置开始的所有内容
			rf.logs = rf.logs[:ridx]
			rf.logs = append(rf.logs, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.logs) {
			// 没有发生冲突但长度更长了, 直接拼接
			rf.logs = append(rf.logs, args.Entries[idx:]...)
			break
		}
	}
	rf.persist()
	rf.status = "follower"
	reply.Success = true
	//rf.term = args.Term
	reply.Term = rf.term
	reply.AppState = AppNormal

	// rf.voteCount = 0
	// rf.voteFor = -1
	//

	// fmt.Printf("")
	if args.LeaderCommit > rf.commitIndex {
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs))-1))
		rf.condApply.Signal()
	}

	DPrintf("??%d任务.term :%d  对面来的长度：%d,现在长度%d,最终长度：%d %+v,%+v\n", rf.me, rf.term, args.LeaderCommit, rf.lastApplied, rf.commitIndex, rf.logs, args.Entries)
}

// 接受回来的信息
func getCommandSize(cmd interface{}) int {
	switch v := cmd.(type) {
	case string:
		return len(v)
	case []byte:
		return len(v)
	case int, int32, int64, uint, uint32, uint64:
		return 8 // 假设用8字节存储数字
	default:
		// 其他类型处理
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(cmd)
		return buf.Len()
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int, pid int) {

	// fmt.Printf("[	sendAppendEntries func-rf(%v)	] send to rf(%v) %d\n", rf.me, server, reply.AppState)

	// fmt.Printf("开始发送%d任务.term :%d 现在长度%d,最终长度：%d %+v\n", rf.me, server, rf.lastApplied, rf.commitIndex, len(rf.logs))
	for i := 0; i < len(args.Entries); i++ {
		// fmt.Printf("args.Entries[%d]:%+v\n", i, getCommandSize(args.Entries[i].Command))
	}
	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.term || rf.status != "leader" {
		return
	}

	if reply.Term > rf.term {
		// 回复了更新的term, 表示自己已经不是leader了
		// 易错点: 这里也应该进行重新选举而不是直接转化为follower, 因为这可能是来自一个孤立节点的返回值
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 转化为Follower\n", rf.me, server, reply.Term)

		rf.term = reply.Term
		rf.status = "follower"
		rf.voteFor = -1
		rf.voteCount = 0
		rf.persist()
		// rf.timeStamp = time.Now()
		// rf.ResetTimer()
		// rf.persist()
		return
	}

	// 对reply的返回状态进行分支
	if reply.Success {
		// server回复成功
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// 需要判断是否可以commit
		N := len(rf.logs) - 1

		for N > rf.commitIndex {
			count := 1 // 1表示包括了leader自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.logs[N].Term == rf.term {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				// +1 表示包括自身
				// 如果至少一半的follower回复了成功, 更新commitIndex
				break
			}
			N -= 1
		}

		// if N > rf.commitIndex {
		rf.commitIndex = N
		rf.condApply.Signal()
		rf.persist()
		// }

		return
	}

	if reply.Term == rf.term && rf.status == "leader" {
		// term仍然相同, 且自己还是leader, 表名对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项
		// 快速回退的处理
		if reply.XTerm == -1 {
			// PrevLogIndex这个位置在Follower中不存在
			DPrintf("leader %v 收到 server %v 的回退请求, 原因是log过短, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, server, server, rf.nextIndex[server], server, reply.XLen)
			rf.nextIndex[server] = reply.XLen
			return
		}

		// 防止数组越界
		// if rf.nextIndex[serverTo] < 1 || rf.nextIndex[serverTo] >= len(rf.log) {
		// 	rf.nextIndex[serverTo] = 1
		// }
		i := rf.nextIndex[server] - 1
		for i > 0 && rf.logs[i].Term > reply.XTerm {
			i -= 1
		}
		if rf.logs[i].Term == reply.XTerm {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己也有

			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的最后一个XTerm索引为%v, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, server, reply.XTerm, reply.XIndex, i, server, rf.nextIndex[server], server, i+1)
			rf.nextIndex[server] = i + 1
		} else {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己没有
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的XTerm不存在, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, server, reply.XTerm, reply.XIndex, server, rf.nextIndex[server], server, reply.XIndex)
			rf.nextIndex[server] = reply.XIndex
		}
		rf.persist()
		return
	}
	rf.persist()
	return
}

// example RequestVote RPC handler.
// 投票给别人 没问题
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//默认不同意投票
	reply.VoteGranted = false
	reply.Term = rf.term

	//情况1: 请求投票的raft的term太小,不给投票
	if args.Term < rf.term {
		// reply.Term = rf.term
		// fmt.Printf("投票太小!	%d号任务(term:%d)收到来自%d号任务(term:%d)\n", rf.me, rf.term, args.CandidateId, args.Term)
		return
	}

	//情况2: 请求投票的raft的term大 将自己的term更新,如果是自己是candidate状态,转为follower
	if args.Term > rf.term {
		rf.term = args.Term
		// fmt.Printf("%d的任务,term:%d,转为follower\n", rf.me, rf.term)
		//这里本来有投票的代码实现,放在了下面的情况3一起处理
		rf.voteFor = -1
		rf.voteCount = 0
		reply.Term = rf.term
		rf.status = "follower"
		rf.persist()
		// rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		// rf.electionTimer.Reset(rf.overtime)
		// rf.electionTimerreset()
		// return
	}
	currentLogIndex := len(rf.logs) - 1
	// 如果currentLogIndex下标不是-1就把term赋值过来
	currentLogTerm := rf.logs[currentLogIndex].Term

	//情况3: 请求投票的raft的term和自己相等情况下,如果rf.voteFor == -1,则投票
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > currentLogTerm ||
			(args.LastLogIndex >= currentLogIndex && args.LastLogTerm == currentLogTerm) {
			rf.voteFor = args.CandidateId
			rf.term = args.Term
			// rf.voteFor = -1
			rf.voteCount = 0
			reply.VoteGranted = true
			rf.persist()
			// fmt.Printf("投票成功!	%d号任务(term:%d)收到来自%d号任务(term:%d)\n", rf.me, rf.term, args.CandidateId, args.Term)
			// rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
			// rf.electionTimer.Reset(rf.overtime) //重新设置投票倒计时
			rf.ResetElectionTimer()
			return
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.term

}

func (rf *Raft) Ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.ElectionIsTimeout() {
			rf.status = "candidate"
			rf.startelection()
			rf.ResetElectionTimer()
		}
		rf.mu.Unlock()
		t := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(t) * time.Millisecond)
	}
}
func (rf *Raft) HeartBeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.HeartBeatIsTimeout() {
			if rf.status == "leader" {
				rf.sendHeartBeat()
				rf.ResetHeart()
			}
		}
		rf.mu.Unlock()
		t := 30 + (rand.Int63() % 5)
		time.Sleep(time.Duration(t) * time.Millisecond)
		// time.Sleep(time.Duration(rf.HeartBeatTimeoutPeriod) * time.Millisecond)
	}
}

// 开始选举 ?
func (rf *Raft) startelection() {
	rf.electionTimerreset()
	rf.term++
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		Args := RequestVoteArgs{
			Term:         rf.term,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1, //最后一个日志的索引 包含未提交的
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}

		// fmt.Printf("发起投票! 	第%d号任务(term:%d)向第%d号任务发起投票\n", rf.me, rf.term, i)
		//以线程的方式发起投票,前面参考文献里面关于锁那个部分提到了
		go rf.sendRequestVote(i, &Args, &reply)
	}
}

// 发送新心跳 没问题
func (rf *Raft) sendHeartBeat() {
	appendNums := 1
	// rf.heartbeatTimer.Reset(HeartBeatTimeout)
	for j, _ := range rf.peers {
		if j == rf.me {
			continue
		}
		p := 0
		appendEntriesArgs := AppendEntriesArgs{
			Term:         rf.term,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		//一开始初始化为rf.nextIndex[i] = len(rf.logs) + 1，然后发送的是空的日志
		// if rf.nextIndex[j] == len(rf.logs)+1 {
		// 	fmt.Printf("rf.nextIndex[j]:%d, len(rf.logs):%d\n", rf.nextIndex[j], len(rf.logs))
		// }
		appendEntriesArgs.Entries = rf.logs[rf.nextIndex[j]:]

		// 代表已经不是初始值0
		if rf.nextIndex[j] > 0 {
			appendEntriesArgs.PrevLogIndex = rf.nextIndex[j] - 1
		}

		if appendEntriesArgs.PrevLogIndex > 0 {
			//fmt.Println("len(rf.log):", len(rf.logs), "PrevLogIndex):", args.PrevLogIndex, "rf.nextIndex[i]", rf.nextIndex[i])
			appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex].Term

		}
		DPrintf("leader %v 开始向 server %v 广播新的心跳, nextIndex[%v]=%v, args = %+v \n", rf.me, j, j, rf.nextIndex[j], appendEntriesArgs)
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(j, &appendEntriesArgs, &reply, &appendNums, p)
		p++
	}
}

// 请求给我投票 没问题
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// fmt.Printf("%d任务,%d term,开始选举\n", rf.me, rf.term)
	// fmt.Printf("	同意投票! 	%d号任务(term:%d) 同意给	%d号任务(term:%d) 投票\n", server, rf.term, rf.me, reply.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for ok == false {

		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.term { // 网络堵塞导致未及时收到过时信息
		return false
	}
	//发送的term比自己的term大 说明自己已经过时
	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.status = "follower"
		rf.voteCount = 0
		rf.voteFor = -1
		rf.persist()
		return false
	}
	rf.electionTimerreset()
	// rf.timer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)
	if reply.VoteGranted == true {
		if rf.status == "follower" {
			rf.voteFor = -1
			rf.voteCount = 0
			rf.persist()
			return ok
		}
		if rf.voteCount <= (len(rf.peers))/2 {
			rf.voteCount++
			rf.persist()
		}
		if rf.voteCount > (len(rf.peers))/2 {
			if rf.status == "leader" {
				return ok
			}
			if rf.status != "candidate" {
				return ok
			}
			DPrintf("新leader!!     第%d号任务已经有选票%d,已经进入leader状态\n", rf.me, rf.voteCount)
			rf.status = "leader"
			// rf.heartbeatTimer.Reset(HeartBeatTimeout)
			rf.ResetHeart()
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
		}
		rf.persist()
	}
	return ok
}

const CommitCheckTimeInterval = time.Duration(50) * time.Millisecond // 检查是否可以commit的间隔
func (rf *Raft) CommitChecker() {
	// 检查是否有新的commit
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			if rf.lastApplied >= len(rf.logs) {
				fmt.Println(len(rf.logs), rf.lastApplied)
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyChan <- msg
			// fmt.Printf("%d任务.term :%d 现在长度%d,最终长度：%d %+v\n", rf.me, rf.term, rf.lastApplied, rf.commitIndex, msg)
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(CommitCheckTimeInterval)
	}
}

// 开始没问题
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	if rf.killed() {
		return index, term, false
	}
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader，直接返回
	if rf.status != "leader" {
		return index, term, false
	}
	appendLog := LogEntry{Term: rf.term, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs) - 1
	term = rf.term

	rf.persist()
	// rf.ResetHeart()
	// fmt.Printf("!!%d任务.term :%d 现在长度%d,最终长度：%d %+v\n", rf.me, rf.term, rf.lastApplied, rf.commitIndex, len(rf.logs))

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// rf.killCh <- struct{}{}
	// close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 没问题
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.status = "follower"
	rf.term = 0
	// fmt.Println("start!")
	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.applyChan = applyCh
	rf.voteCount = 0
	// initialize from state persisted before a crash
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logs) // raft中的index是从1开始的
	}
	rf.overtime = time.Duration(300+rand.Intn(150)) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.overtime)
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)
	rf.LastHeartBeatTime = time.Now()
	rf.LastElectionTimeoutTime = time.Now()
	rf.readPersist(persister.ReadRaftState())
	rf.condApply = sync.NewCond(&rf.mu)
	go rf.Ticker()
	go rf.HeartBeatTicker()
	go rf.CommitChecker()
	return rf
}
