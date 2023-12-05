package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugRF int = 1

type LogEntry struct {
	Command any
	Term    int
}

type Tstate int

const (
	Follower Tstate = iota
	Candidate
	Leader
	Dead
)

func (s Tstate) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type Raft struct {
	mu sync.Mutex

	//服务器id
	id int
	// 集群中所有服务器
	peerIds []int
	// raft所在服务器
	server *Server

	//storage相当于一个记录raft状态的数据库
	storage Storage

	// 将committed的日志条目，应用到状态机，使用的管道
	commitChan chan<- CommitEntry //这是一个连接外部的通道，所以这个通道一定是在外面创建的

	//内部通道用来通知其它goroutine有新确认的日志可以应用到状态机了
	newCommitReadyChan chan struct{}

	//内部信号 当需要发送新的AE RPC时，传入信号
	triggerAEChan chan struct{}

	//在所有服务器上持久性raft状态 （需要持久保存的）
	currentTerm int
	votedFor    int
	log         []LogEntry

	//服务器上易失性raft状态 （不需要持久保存的）
	commitIndex        int //已确认日志索引
	lastApplied        int // 已应用日志索引
	state              Tstate
	electionResetEvent time.Time

	// 在leader服务器上的易失性状态
	nextIndex  map[int]int //记录了leader将要发给每个节点的下一个日志条目的索引
	matchIndex map[int]int //记录了leader认为每个节点已经复制的日志条目的索引
	// 通过它们可以确认日志的复制情况
}

// NewRaft ready channel 当所有节点准备好后，并且能安全的启动状态机时会收到信号。
func NewRaft(id int, peersIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *Raft {
	rf := new(Raft)
	rf.id = id
	rf.peerIds = peersIds
	rf.server = server
	rf.commitChan = commitChan
	rf.newCommitReadyChan = make(chan struct{}, 16)
	rf.triggerAEChan = make(chan struct{})
	rf.storage = storage
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	if storage.HasData() {
		rf.restoreFromStorage(rf.storage)
	}

	go func() {
		<-ready
		rf.mu.Lock()
		rf.electionResetEvent = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}()

	go rf.commitChanSender() //外部通道
	return rf
}

// 获取当前raft状态
func (rf *Raft) Report() (id int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.id, rf.currentTerm, rf.state == Leader
}

// 该函数返回很快，但可能会需要一点时间等待所有go routine结束
func (rf *Raft) Stop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Dead
	rf.dlog("becomes dead")
	close(rf.newCommitReadyChan)
}

// 打印调试日志记录
func (rf *Raft) dlog(format string, args ...any) {
	if DebugRF > 0 {
		format = fmt.Sprintf("[%d] ", rf.id) + format
		log.Printf(format, args...)
	}
}

// 设置随机的超时时间
func (rf *Raft) electionTimeout() time.Duration {
	//总体来说，这段代码的作用是在正常情况下生成一个随机的选举超时时间，
	//但如果设置了特定的环境变量并且随机数满足条件，就会返回一个固定的选举超时时间。、
	//这种设计可能是为了在某些情况下，比如测试环境下，更容易触发选举，以确保Raft算法在各种情况下都能正确工作。
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

//	第一步
//
// 实现随机选举超时计时器
// 计时开始时间是实时更新的，每一个rpc请求都会重置
func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.electionTimeout()
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	rf.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this raft becomes a candidate（候选人也是需要计时器的）
	// 对于follower来说，这是一直在运行的
	ticker := time.NewTicker(10 * time.Millisecond) //相当于时间间隔器
	defer ticker.Stop()
	for {
		<-ticker.C // 每十把通道数据拿出来，以便循环继续

		rf.mu.Lock()
		if rf.state != Candidate && rf.state != Follower { // dead或者leader 不需要计时器
			rf.dlog("in election timer state=%s, bailing out", rf.state)
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm { // 计时过程中任期发生了变化，退出重新计时
			rf.dlog("in election timer term changed from %d to %d, bailing out", termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		// 开始计时
		if elapsed := time.Since(rf.electionResetEvent); elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// 第二步 带锁
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	savedCurrentTerm := rf.currentTerm //中途其它事情可能会改变节点任期
	rf.electionResetEvent = time.Now() //开启一次选举也要重置计时器
	rf.votedFor = rf.id
	rf.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rf.log)

	votesReceived := 1

	// 给所有服务器发送RequestVote RPC
	for _, peerId := range rf.peerIds {
		go func(peerId int) {
			rf.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm() // 日志任期跟节点任期是两回事
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			rf.dlog("sending RequestVote to %d: %+v", peerId, args)

			var reply RequestVoteReply
			if err := rf.server.Call(peerId, "Raft.RequestVote", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.dlog("received RequestVoteReply %+v", reply)

				if rf.state != Candidate { //确保自己仍是候选人
					rf.dlog("while waiting for reply, state = %v", rf.state) // 等待期间状态发生改变
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.dlog("term out of date in RequestVoteReply") //自己任期过期了
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm { // 注意：回复者是更小任期怎么处理
					if reply.VoteGranted { // 处理之后别人要么等于要么大于
						votesReceived++
						if votesReceived*2 > len(rf.peerIds)+1 {
							rf.dlog("wins election with %d votes", votesReceived)
							rf.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// 开启选举时间超时
	go rf.runElectionTimer()
}

// 第三步
func (rf *Raft) startLeader() {
	rf.state = Leader

	for _, peerId := range rf.peerIds {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = -1
	}
	rf.dlog("becomes Leader; term=%d, log=%v", rf.currentTerm, rf.log)

	// 开启一个不断发送心跳的协程
	//要么50ms发一次
	//要么triggerAEChan有信号（有新的日志条目
	go func(heartbeatTimeout time.Duration) {
		rf.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout) // 心跳要比选举超时低一个数量级
		defer t.Stop()

		// leader 间歇地发送心跳
		for {
			doSend := false //是否发送AE rpc
			select {
			case <-t.C:
				doSend = true

				t.Stop()
				t.Reset(heartbeatTimeout) //重新设置定时器
			case _, ok := <-rf.triggerAEChan: //当通道关闭返回false
				if ok {
					doSend = true
				} else {
					return
				}

				if !t.Stop() { //发送了AE rpc重置心跳计时器
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				rf.mu.Lock()
				if rf.state != Leader { //不断检查自己还是否为leader
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

// 将心跳与日志复制结合到一起
func (rf *Raft) leaderSendAEs() {
	rf.mu.Lock()
	if rf.state != Leader {
		return
	}
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for _, peerId := range rf.peerIds {
		go func(peerId int) {
			rf.mu.Lock()
			ni := rf.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[ni:] //要发送的日志条目

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			rf.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)

			var reply AppendEntriesReply
			if err := rf.server.Call(peerId, "Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					rf.dlog("term out of date in heartbeat reply")
					rf.becomeFollower(reply.Term)
					return
				}

				if rf.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						rf.nextIndex[peerId] = ni + len(entries)
						rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
						rf.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, rf.nextIndex, rf.matchIndex)

						savedCommitIndex := rf.commitIndex                  // 用来判断是否有新日志确认
						for i := rf.commitIndex + 1; i < len(rf.log); i++ { // 统计复制是否过半
							if rf.log[i].Term == rf.currentTerm { //不能确认之前任期的索引
								matchCount := 1
								for _, peerId := range rf.peerIds {
									if rf.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(rf.peerIds)+1 {
									rf.commitIndex = i
								}
							}
						}
						if rf.commitIndex != savedCommitIndex {
							rf.dlog("leader sets commitIndex := %d", rf.commitIndex)
							rf.newCommitReadyChan <- struct{}{}
							rf.triggerAEChan <- struct{}{} //告诉follower有新日志确认了，你也可以马上确认了
						}
					} else { //必须前面日志一致才接收新日志
						if reply.ConflictTerm > 0 {
							lastIndexOfTerm := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm { //conflictTerm是没有问题的，下一个任期才有问题
									lastIndexOfTerm = i
									break
								}
								if lastIndexOfTerm >= 0 {
									rf.nextIndex[peerId] = lastIndexOfTerm + 1
								} else {
									rf.nextIndex[peerId] = reply.ConflictIndex
								}
							}
						} else {
							rf.nextIndex[peerId] = reply.ConflictIndex
						}
						rf.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

// 应用到状态机可能是一个复杂阻塞的过程，所以设计成并发，一直挂在后台，而不是有新确认再来调用这个函数
func (rf *Raft) commitChanSender() {
	for range rf.newCommitReadyChan {
		//有新确认条目需要apply
		rf.mu.Lock()
		savedTerm := rf.currentTerm
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		rf.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			rf.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	rf.dlog("commitChanSender done")
}

func (rf *Raft) becomeFollower(term int) {
	rf.dlog("becomes Follower with term=%d; log=%v", term, rf.log)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionResetEvent = time.Now()

	go rf.runElectionTimer()
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int //保证只给有最新条目的投票，这样保证了leader一般是集群中最新的节点
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote 第四步
// handle RequestVote RPC
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return nil
	}

	rf.dlog("handle RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, rf.currentTerm, rf.votedFor)
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()

	if args.Term > rf.currentTerm { //任期小更新完任期再投票
		rf.dlog("...higher term in RequestVote")
		rf.becomeFollower(args.Term) //这里处理了比发送方任期小的问题，最终会变得任期一样
	}

	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || //是否最新检查，先看最新日志任期再看最新日志索引
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persisToStorage()
	rf.dlog("...RequestVote reply: %+v", reply)

	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int //这两项用于一致性检查
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool //当prev能匹配的时候返回true，用于一致性检查

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)非必要
	//一致性检查本来一次只检查一个，通过这个一次返回多个
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries 该请求只有leader才能发
// handle AppendEntries 请求
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return nil
	}
	rf.dlog("handle AppendEntries: %+v", args)

	if args.Term > rf.currentTerm {
		rf.dlog("...higher term in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower { //前面的逻辑保证了 永远不会出现两个leader
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()

		//一致性检查
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1 //日志插入点
			newEntriesIndex := 0                    //args日志里的索引

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex要么指向日志尾部，要么指向任期不匹配的地方
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				rf.dlog("...inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				rf.dlog("... log is now: %v", rf.log)
			}

			//设置该节点commit索引
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				rf.dlog("...setting commitIndex=%d", rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}
		} else {
			//一致性检查失败 还要往前倒
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = rf.log[args.PrevLogTerm].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = rf.currentTerm //我的任期比你大拒绝你的心跳请求，不会重置计时器
	rf.persisToStorage()
	rf.dlog("AppendEntries reply: %+v", *reply)

	return nil
}

// CommitEntry 表示已经committed的命令，通过commit channel传递给服务器上的其它服务（状态机
type CommitEntry struct {
	Command any

	//committed命令的索引
	Index int
	//节点任期
	Term int
}

func (rf *Raft) Submit(command interface{}) bool {
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.dlog("Submit received by %v: %v", rf.state, command)
	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.persisToStorage()
		rf.dlog("...log=%v", rf.log)
		rf.mu.Unlock()
		rf.triggerAEChan <- struct{}{} //防止死锁
		return true
	}

	rf.mu.Unlock()
	return false
}

// 返回最后日志索引和最后日志任期
// 需要锁
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		return lastIndex, rf.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// 持久化的数据只有三个
func (rf *Raft) restoreFromStorage(storage Storage) {
	if termData, found := rf.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&rf.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}

	if votedData, found := rf.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&rf.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}

	if logData, found := rf.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&rf.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (rf *Raft) persisToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(rf.votedFor); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(rf.log); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("log", logData.Bytes())
}
