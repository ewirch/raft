// Core Raft implementation - Consensus Module.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

const DebugCM = 1

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command and it can
// be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command is committed.
	Term int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
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

type LogEntry struct {
	Command interface{}
	Term    int
}

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {
	// Go routines use this channel to borrow the shared state object before access, this way
	// concurrent go routines synchronize themselves. When they are done with the object,
	// they return it. This guarantees that only one go routine at a time can access the
	// shared state object. No data race can happen.
	//
	// There are some rules to be followed:
	//
	// 1. The shared state object should be borrowed by reading from the channel. The same
	// method must return the borrowed object to the channel using 'defer' keyword.
	//
	// 2. The borrowing channel itself may not be part of the shared state object.
	//
	// 3. It must be impossible to try to borrow twice from the channel (in one singe go
	// routine). Therefore, a method should have access to either the shared state object
	// (mutableConsensusModule) or to the channel hosting the shared state object
	// (ConsensusModule), but not to both at the same time. Exception: methods retrieving
	// (and returning) the shared state object and delegating to shared state methods.
	//
	// 4. To simplify reasoning, the delegating methods should not contain any other logic.
	// This prevents from accidentally leaking access to the borrowing channel in to
	// shared state methods.
	//
	// 5. Shared state methods may not create other go routines and pass the shared state
	// object reference to them (or references to any other variables from in shared
	// state object). This would violate rule 1.
	//
	// Why not a mutex? It is hard to reason about whether a method runs in locked context
	// (mutex was acquired), using mutexes. It easy if the method, you are looking at, locks
	// the mutex; but if the method calls other methods, and those call other methods, it
	// becomes harder. When you get back to your code months later, or when a coder
	// unfamiliar with your code looks at it, it is hard to tell. You have to analyze call
	// chains to find out if the method runs in a correctly locked context or not. This
	// becomes a maintainability burden and a source of bugs.
	//
	// This problem does not arise using object borrowing. If the method has a reference to
	// the object, it is allowed to access/modify the object. You can tell from the method
	// declaration alone if the method runs in "locked" context.
	//
	// It is not clear which variables a mutex guards. Access to some variables is thread
	// safe, to others is not. A shared state object groups them. It is clear, in order to
	// get access to shared state variables, you have to get the shared state object.
	mu chan *mutableConsensusModule

	sharedStaticConsensusModule

	// server is the server containing this CM. It's used to issue RPC calls
	// to peers.
	server *Server

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}

	// triggerAEChan is an internal notification channel used to trigger
	// sending new AEs to followers when interesting changes occurred.
	triggerAEChan chan struct{}
}

type sharedStaticConsensusModule struct {
	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// goRoutines receives new go routines, which should be started with ConsensusModule reference.
	// A background go routine (goRoutinesStarter) reads this channel and starts the routines.
	goRoutines chan func(cm *ConsensusModule)
}

type mutableConsensusModule struct {
	sharedStaticConsensusModule

	// storage is used to persist state.
	storage Storage

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine. commitChan is going to be used by the
// CM to send log entries that have been committed by the Raft cluster.
func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	mutable := new(mutableConsensusModule)
	mutable.id = id
	mutable.peerIds = peerIds
	mutable.storage = storage
	mutable.state = Follower
	mutable.votedFor = -1
	mutable.commitIndex = -1
	mutable.lastApplied = -1
	mutable.nextIndex = make(map[int]int)
	mutable.matchIndex = make(map[int]int)
	mutable.goRoutines = make(chan func(cm *ConsensusModule), 10)

	cm := new(ConsensusModule)
	cm.id = mutable.id
	cm.peerIds = mutable.peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.goRoutines = mutable.goRoutines
	cm.mu = make(chan *mutableConsensusModule, 1)
	cm.mu <- mutable

	if mutable.storage.HasData() {
		mutable.restoreFromStorage()
	}

	go cm.goRoutinesStarter()
	go func() {
		// The CM is dormant until ready is signaled; then, it starts a countdown
		// for leader election.
		<-ready
		cm.resetElectionTimer()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

func (cm *ConsensusModule) goRoutinesStarter() {
	for routine := range cm.goRoutines {
		go routine(cm)
	}
	cm.dlog("goRoutinesStarter done")
}

func (cm *ConsensusModule) resetElectionTimer() {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()
}

func (m *mutableConsensusModule) resetElectionTimer() {
	m.electionResetEvent = time.Now()
}

// Report reports the state of this CM.
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	id = cm.id
	term, isLeader = mutable.Report()
	return
}

func (m *mutableConsensusModule) Report() (term int, isLeader bool) {
	return m.currentTerm, m.state == Leader
}

// Submit submits a new command to the CM. This function doesn't block; clients
// read the commit channel passed in the constructor to be notified of new
// committed entries. It returns true iff this CM is the leader - in which case
// the command is accepted. If false is returned, the client will have to find
// a different CM to submit this command to.
func (cm *ConsensusModule) Submit(command interface{}) bool {
	if cm.submitIfLeader(command) {
		cm.triggerAEChan <- struct{}{}
		return true
	}
	return false
}

func (cm *ConsensusModule) submitIfLeader(command interface{}) bool {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.submitIfLeader(command)
}

func (m *mutableConsensusModule) submitIfLeader(command interface{}) bool {
	m.dlog("Submit received by %v: %v", m.state, command)
	if m.state == Leader {
		m.log = append(m.log, LogEntry{Command: command, Term: m.currentTerm})
		m.persistToStorage()
		m.dlog("... log=%v", m.log)
		return true
	}
	return false
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (cm *ConsensusModule) Stop() {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	mutable.stop()
	close(cm.newCommitReadyChan)
	close(cm.goRoutines)
}

func (m *mutableConsensusModule) stop() {
	m.state = Dead
	m.dlog("becomes Dead")
}

// restoreFromStorage restores the persistent stat of this CM from storage.
// It should be called during constructor, before any concurrency concerns.
func (m *mutableConsensusModule) restoreFromStorage() {
	if termData, found := m.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&m.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := m.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&m.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := m.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&m.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// persistToStorage saves all of CM's persistent state in cm.storage.
// Expects cm.mu to be locked.
func (m *mutableConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(m.currentTerm); err != nil {
		log.Fatal(err)
	}
	m.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(m.votedFor); err != nil {
		log.Fatal(err)
	}
	m.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(m.log); err != nil {
		log.Fatal(err)
	}
	m.storage.Set("log", logData.Bytes())
}

// dlog logs a debugging message is DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (m *mutableConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", m.id) + format
		log.Printf(format, args...)
	}
}

// See figure 2 in the paper.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.requestVote(args, reply)
}

func (m *mutableConsensusModule) requestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if m.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := m.lastLogIndexAndTerm()
	m.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, m.currentTerm, m.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > m.currentTerm {
		m.dlog("... term out of date in RequestVote")
		m.becomeFollower(args.Term)
	}

	if m.currentTerm == args.Term &&
		(m.votedFor == -1 || m.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		m.votedFor = args.CandidateId
		m.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = m.currentTerm
	m.persistToStorage()
	m.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// See figure 2 in the paper.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if cm.appendEntries(args, reply) {
		cm.newCommitReadyChan <- struct{}{}
	}
	return nil
}

func (cm *ConsensusModule) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) (commitIndexChanged bool) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.appendEntries(args, reply)
}

func (m *mutableConsensusModule) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) (commitIndexChanged bool) {
	commitIndexChanged = false

	if m.state == Dead {
		return
	}
	m.dlog("AppendEntries: %+v", args)

	if args.Term > m.currentTerm {
		m.dlog("... term out of date in AppendEntries")
		m.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == m.currentTerm {
		if m.state != Follower {
			m.becomeFollower(args.Term)
		}
		m.electionResetEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
		// vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(m.log) && args.PrevLogTerm == m.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(m.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if m.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				m.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				m.log = append(m.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				m.dlog("... log is now: %v", m.log)
			}

			// Set commit index.
			if args.LeaderCommit > m.commitIndex {
				m.commitIndex = intMin(args.LeaderCommit, len(m.log)-1)
				m.dlog("... setting commitIndex=%d", m.commitIndex)
				commitIndexChanged = true
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(m.log) {
				reply.ConflictIndex = len(m.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// m.log[PrevLogIndex].
				reply.ConflictTerm = m.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if m.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = m.currentTerm
	m.persistToStorage()
	m.dlog("AppendEntries reply: %+v", *reply)
	return
}

// electionTimeout generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	termStarted := cm.getTerm()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this CM becomes a candidate
	// In a follower, this typically keeps running in the background for the
	// duration of the CM's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		stop := cm.electionTimerTick(termStarted, timeoutDuration)
		if stop {
			return
		}
	}
}

func (cm *ConsensusModule) getTerm() int {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()
	return mutable.getTerm()
}

func (m *mutableConsensusModule) getTerm() int {
	return m.currentTerm
}

func (cm *ConsensusModule) electionTimerTick(termStarted int, timeoutDuration time.Duration) bool {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.electionTimerTick(termStarted, timeoutDuration)
}

func (m *mutableConsensusModule) electionTimerTick(termStarted int, timeoutDuration time.Duration) bool {
	if m.state != Candidate && m.state != Follower {
		m.dlog("in election timer state=%s, bailing out", m.state)
		return true
	}

	if termStarted != m.currentTerm {
		m.dlog("in election timer term changed from %d to %d, bailing out", termStarted, m.currentTerm)
		return true
	}

	// Start an election if we haven't heard from a leader or haven't voted for
	// someone for the duration of the timeout.
	if elapsed := time.Since(m.electionResetEvent); elapsed >= timeoutDuration {
		m.startElection()
		return true
	}
	return false
}

// startElection starts a new election with this CM as a candidate.
// Expects cm.mu to be locked.
func (m *mutableConsensusModule) startElection() {
	m.state = Candidate
	m.currentTerm += 1
	savedCurrentTerm := m.currentTerm
	m.electionResetEvent = time.Now()
	m.votedFor = m.id
	m.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, m.log)

	var votesReceived int32 = 1

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range m.peerIds {
		// capture peerId to use in lambda
		peerId := peerId
		m.goRoutines <- func(cm *ConsensusModule) {
			cm.sendRequestVoteToPeer(peerId, savedCurrentTerm, &votesReceived)
		}
	}

	// Run another election timer, in case this election is not successful.
	m.goRoutines <- func(cm *ConsensusModule) {
		cm.runElectionTimer()
	}
}

func (cm *ConsensusModule) sendRequestVoteToPeer(peerId int, savedCurrentTerm int, votesReceived *int32) {
	savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()

	args := RequestVoteArgs{
		Term:         savedCurrentTerm,
		CandidateId:  cm.id,
		LastLogIndex: savedLastLogIndex,
		LastLogTerm:  savedLastLogTerm,
	}

	cm.dlog("sending RequestVote to %d: %+v", peerId, args)
	var reply RequestVoteReply
	if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
		cm.processRequestVoteReply(reply, savedCurrentTerm, votesReceived)
	}
}

func (cm *ConsensusModule) processRequestVoteReply(reply RequestVoteReply, savedCurrentTerm int, votesReceived *int32) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	mutable.processRequestVoteReply(reply, savedCurrentTerm, votesReceived)
}

func (m *mutableConsensusModule) processRequestVoteReply(reply RequestVoteReply, savedCurrentTerm int, votesReceived *int32) {
	m.dlog("received RequestVoteReply %+v", reply)

	if m.state != Candidate {
		m.dlog("while waiting for reply, state = %v", m.state)
		return
	}

	if reply.Term > savedCurrentTerm {
		m.dlog("term out of date in RequestVoteReply")
		m.becomeFollower(reply.Term)
		return
	} else if reply.Term == savedCurrentTerm {
		if reply.VoteGranted {
			votes := int(atomic.AddInt32(votesReceived, 1))
			if votes*2 > len(m.peerIds)+1 {
				// Won the election!
				m.dlog("wins election with %d votes", votes)
				m.startLeader()
				return
			}
		}
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()
	return mutable.lastLogIndexAndTerm()
}

// becomeFollower makes cm a follower and resets its state.
// Expects cm.mu to be locked.
func (m *mutableConsensusModule) becomeFollower(term int) {
	m.dlog("becomes Follower with term=%d; log=%v", term, m.log)
	m.state = Follower
	m.currentTerm = term
	m.votedFor = -1
	m.electionResetEvent = time.Now()

	m.goRoutines <- func(cm *ConsensusModule) {
		cm.runElectionTimer()
	}
}

// startLeader switches cm into a leader state and begins process of heartbeats.
// Expects cm.mu to be locked.
func (m *mutableConsensusModule) startLeader() {
	m.state = Leader

	for _, peerId := range m.peerIds {
		m.nextIndex[peerId] = len(m.log)
		m.matchIndex[peerId] = -1
	}
	m.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", m.currentTerm, m.nextIndex, m.matchIndex, m.log)

	// This goroutine runs in the background and sends AEs to peers:
	// * Whenever something is sent on triggerAEChan
	// * ... Or every 50 ms, if no events occur on triggerAEChan
	m.goRoutines <- func(cm *ConsensusModule) {
		cm.beLeader(50 * time.Millisecond)
	}
}

func (cm *ConsensusModule) beLeader(heartbeatTimeout time.Duration) {
	// Immediately send AEs to peers.
	cm.leaderSendAEs()

	t := time.NewTimer(heartbeatTimeout)
	defer t.Stop()
	for {
		doSend := false
		select {
		case <-t.C:
			doSend = true

			// Reset timer to fire again after heartbeatTimeout.
			t.Stop()
			t.Reset(heartbeatTimeout)
		case _, ok := <-cm.triggerAEChan:
			if ok {
				doSend = true
			} else {
				return
			}

			// Reset timer for heartbeatTimeout.
			if !t.Stop() {
				<-t.C
			}
			t.Reset(heartbeatTimeout)
		}

		if doSend {
			if !cm.isLeader() {
				return
			}
			cm.leaderSendAEs()
		}
	}
}

func (cm *ConsensusModule) isLeader() bool {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()
	return mutable.isLeader()
}

func (m *mutableConsensusModule) isLeader() bool {
	return m.state == Leader
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts cm's state.
func (cm *ConsensusModule) leaderSendAEs() {
	m := <-cm.mu
	defer func() { cm.mu <- m }()

	m.leaderSendAEs()
}

func (m *mutableConsensusModule) leaderSendAEs() {
	if m.state == Dead {
		return
	}
	savedCurrentTerm := m.currentTerm
	for _, peerId := range m.peerIds {
		// capture peerId to use in lambda
		peerId := peerId
		m.goRoutines <- func(cm *ConsensusModule) {
			cm.leaderSendAEsToPeer(savedCurrentTerm, peerId)
		}
	}
}

func (cm *ConsensusModule) leaderSendAEsToPeer(savedCurrentTerm int, peerId int) {
	ni, args := cm.getAppendEntries(peerId, savedCurrentTerm)
	cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
	var reply AppendEntriesReply
	if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
		if cm.processSendAEsReply(savedCurrentTerm, peerId, reply, ni, args) {
			// Commit index changed: the leader considers new entries to be
			// committed. Send new entries on the commit channel to this
			// leader's clients, and notify followers by sending them AEs.
			cm.newCommitReadyChan <- struct{}{}
			cm.triggerAEChan <- struct{}{}
		}
	}
}

func (cm *ConsensusModule) processSendAEsReply(savedCurrentTerm int, peerId int, reply AppendEntriesReply, ni int, args AppendEntriesArgs) (commitIndexChanged bool) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.processSendAEsReply(savedCurrentTerm, peerId, reply, ni, args)
}

func (m *mutableConsensusModule) processSendAEsReply(savedCurrentTerm int, peerId int, reply AppendEntriesReply, ni int, args AppendEntriesArgs) (commitIndexChanged bool) {
	commitIndexChanged = false
	if reply.Term > savedCurrentTerm {
		m.dlog("term out of date in heartbeat reply")
		m.becomeFollower(reply.Term)
		return
	}

	if m.state == Leader && savedCurrentTerm == reply.Term {
		if reply.Success {
			m.nextIndex[peerId] = ni + len(args.Entries)
			m.matchIndex[peerId] = m.nextIndex[peerId] - 1

			savedCommitIndex := m.commitIndex
			for i := m.commitIndex + 1; i < len(m.log); i++ {
				if m.log[i].Term == m.currentTerm {
					matchCount := 1
					for _, peerId := range m.peerIds {
						if m.matchIndex[peerId] >= i {
							matchCount++
						}
					}
					if matchCount*2 > len(m.peerIds)+1 {
						m.commitIndex = i
					}
				}
			}
			m.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, m.nextIndex, m.matchIndex, m.commitIndex)
			if m.commitIndex != savedCommitIndex {
				m.dlog("leader sets commitIndex := %d", m.commitIndex)
				return true
			}
		} else {
			if reply.ConflictTerm >= 0 {
				lastIndexOfTerm := -1
				for i := len(m.log) - 1; i >= 0; i-- {
					if m.log[i].Term == reply.ConflictTerm {
						lastIndexOfTerm = i
						break
					}
				}
				if lastIndexOfTerm >= 0 {
					m.nextIndex[peerId] = lastIndexOfTerm + 1
				} else {
					m.nextIndex[peerId] = reply.ConflictIndex
				}
			} else {
				m.nextIndex[peerId] = reply.ConflictIndex
			}
			m.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
		}
	}
	return
}

func (cm *ConsensusModule) getAppendEntries(peerId int, term int) (int, AppendEntriesArgs) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.getAppendEntries(peerId, term)
}

func (m *mutableConsensusModule) getAppendEntries(peerId int, term int) (int, AppendEntriesArgs) {
	ni := m.nextIndex[peerId]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = m.log[prevLogIndex].Term
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     m.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      m.log[ni:],
		LeaderCommit: m.commitIndex,
	}

	return ni, args
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
func (m *mutableConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(m.log) > 0 {
		lastIndex := len(m.log) - 1
		return lastIndex, m.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// commitChanSender is responsible for sending committed entries on
// cm.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; cm.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// Find which entries we have to apply.
		entries, term, lastApplied := cm.getCommittedAndUnsent()
		cm.dlog("commitChanSender entries=%v, lastApplied=%d", entries, lastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   lastApplied + i + 1,
				Term:    term,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) getCommittedAndUnsent() ([]LogEntry, int, int) {
	mutable := <-cm.mu
	defer func() { cm.mu <- mutable }()

	return mutable.getCommittedAndUnsent()
}

func (m *mutableConsensusModule) getCommittedAndUnsent() ([]LogEntry, int, int) {
	savedTerm := m.currentTerm
	savedLastApplied := m.lastApplied
	var entries []LogEntry
	if m.commitIndex > m.lastApplied {
		entries = m.log[m.lastApplied+1 : m.commitIndex+1]
		m.lastApplied = m.commitIndex
	}
	return entries, savedTerm, savedLastApplied
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
