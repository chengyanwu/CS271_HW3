package client

import (
	"fmt"
	"net"
	"sync"
	"log"
	"os"
	"bufio"
	"strings"
	"time"
	"math/rand"
	"encoding/json"
	"strconv"

	storage "example/users/client/storage"
	disk   "example/users/client/disk"
	crypto "example/users/client/crypto"
)

type Role uint8
type Action uint8
type Rpc uint8

const (
	FOLLOWER Role = iota
	LEADER
	CANDIDATE
	DEAD
)

const (
	GET Action = iota
	PUT
	DELETE
)

const (
	REQUESTVOTE_REQ Rpc = iota
	REQUESTVOTE_REPLY
	APPENDENTRIES_REQ
	APPENDENTRIES_REPLY
)

const (
	DELIM = ":"
	TIMEOUT = 7
)

// implement ToString
func (r Role) String() string {
	switch r {
		case FOLLOWER:
			return "Follower"
		case LEADER:
			return "Leader"
		case CANDIDATE:
			return "Candidate"
		default:
			return "Unknown"
	}
}

// Configuration for a client that implements the RAFT consensus algorithm
type ClientInfo struct {
	ProcessId     int64               // process ID identifier, for encryption purposes
	ClientName    string              // name identifier: A, B, C, D, E
	OutboundConns storage.ConnStorage // outbound connections to other servers
	InboundConns  storage.ConnStorage // inbound connections from other servers
	CurrentTerm   int
	CurrentIndex  int
	ReplicatedLog []LogEntry          // log entries, read from the disk
	VotedFor      string              // name of client voted for leader
	CurrentRole   Role                // 1: follower / 2: leader / 3: candidate
	CurrentLeader LeaderInfo	      // information of the current leader
	VotesReceived []string            // names of channels whose vote were received
	Mu            *sync.Mutex
	LogStore      disk.LogStore        // interact to store logs and other info on disk

	// TODO: StateMachine storage.ReplicatedDictOfDicts
	Faillinks     map[string]net.Conn // Stop sending messages to and ignores messages received from the connections here
	Keys		  crypto.Keys         // public and private keys
	DiskLogger	  *log.Logger         // logs information about AppendEntry RPCs and commits to the state machine
	ElecLogger    *log.Logger		  // logs information about Election RPCs and leader changes
	ConnLogger    *log.Logger         // logs information about outgoing and ingoing messages on connections, and connection disconnects

	LastContact   time.Time           // tracks last contact with the leader

	// Communication channels for RPC
	ReqVoteRequestChan  chan RequestVoteRequest
	ReqVoteResponseChan chan RequestVoteResponse
}

type LogEntry struct {
	Index int
	Term  int
	Command Command
}

type Command struct {
	Key string
	Value string
	Action Action
}

type ConnectionInfo struct {
	Connection net.Conn
	ClientName string
}

type LeaderInfo struct {
	NoLeader           bool
	Mu         		   *sync.Mutex
	ClientName 		   string      // A, B, C, D, E
	InboundConnection  net.Conn
	OutboundConnection net.Conn
}

// PROTOCOL: [identifier:marshalled byte slice]
type Marshaller interface {
	Marshal() []byte
	Demarshal([]byte) 
}

// TODO: declare struct types for PROTOCOL information msgs (should implement the Marshaller interface)
type RequestVoteRequest struct {
	CandidateName string
	CandidateTerm int
	LastLogIndex  int
	LastLogTerm   int
}

func (r *RequestVoteRequest) Marshal() []byte {
	bytes, err := json.Marshal(r)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal RequestVoteRequest struct: %v\n", *r))
	}

	return bytes
}

func (r *RequestVoteRequest) Demarshal(b []byte) {
	json.Unmarshal(b, r)
}

type RequestVoteResponse struct {
	NewTerm     int
	VoteGranted bool
}

func (r *RequestVoteResponse) Marshal() []byte {
	bytes, err := json.Marshal(r)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal RequestVoteResponse struct: %v\n", *r))
	}

	return bytes
}

func (r *RequestVoteResponse) Demarshal(b []byte) {
	json.Unmarshal(b, r)
}

// Check if current client is the leader
func (c *ClientInfo) CheckSelf() bool {
	return c.CurrentLeader.ClientName == c.ClientName
}

func (l *LeaderInfo) clearLeader() {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.NoLeader = true
	l.ClientName = ""
	l.InboundConnection = nil
	l.OutboundConnection = nil
}

func (c *ClientInfo) String() string {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	return fmt.Sprintf("\n===== Client Info =====\n"+
		"Process ID: %d\n"+
		"Client name: %s\n"+
		"Current term: %d\n"+
		"Voted for: %s\n"+
		"Current role: %s\n"+
		"Leader: %s\n"+
		"Outbound connections: %+v\n"+
		"Inbound connections: %+v\n",
		c.ProcessId, c.ClientName, c.CurrentTerm, c.VotedFor, c.CurrentRole.String(), c.CurrentLeader.ClientName, c.OutboundConns.Keys(), c.InboundConns.Keys())
}

// Sets up client info upon first initialization of RAFT algorithm
func (c *ClientInfo) NewRaft(processID int64, name string, clientMu *sync.Mutex, leaderInfoMu *sync.Mutex) {
	c.ProcessId = processID
	c.ClientName = name
	c.OutboundConns = storage.NewConnStorage()
	c.InboundConns = storage.NewConnStorage()
	c.CurrentTerm = 0
	c.CurrentIndex = 0
	c.ReplicatedLog = make([]LogEntry, 0)
	c.VotedFor = ""
	c.CurrentRole = FOLLOWER
	c.CurrentLeader = LeaderInfo{ false, leaderInfoMu, "", nil, nil }
	c.VotesReceived = make([]string, 0)
	c.Mu = clientMu
	c.LogStore = disk.LogStore{} // TODO
	c.Faillinks = make(map[string]net.Conn)
	c.DiskLogger = log.New(os.Stdout, "[FSM]", log.LstdFlags)
	c.ElecLogger = log.New(os.Stdout, "[ELECTION]", log.LstdFlags)
	c.ConnLogger = log.New(os.Stdout, "[CONN]", log.LstdFlags)
	c.LastContact = time.Now()
	c.Keys.New()

	c.ReqVoteRequestChan = make(chan RequestVoteRequest)
	c.ReqVoteResponseChan = make(chan RequestVoteResponse)
}

// Recover client information after crashing
func (c *ClientInfo) Recover() {
	// TODO
}

// Begin listening on the passed connection
func (c *ClientInfo) Listen(connection net.Conn, clientName string) {
	c.ConnLogger.Printf("Listening to inbound client: %s\n", clientName)
	go c.recvIncomingMessages(connection, clientName)
}

// Main RAFT algorithm
func (c *ClientInfo) RAFT() {
	c.ElecLogger.Println("RAFT starting, waiting for elections")
	c.init()
	for {
		// TODO: main RAFT loop
		// switch statement for leader, candidiate, and follower
		switch c.getRole() {
		case FOLLOWER:
			c.follower()
		case CANDIDATE:
			c.candidate()
		case LEADER: 
			// c.leader()
		}
	}
}

func (c *ClientInfo) follower() {
	c.ElecLogger.Printf("State: FOLLOWER\n")
	// start election timer, reset election timeout
	timer, duration := newElectionTimer(TIMEOUT * time.Second)
	c.LastContact = time.Now()

	c.Mu.Lock()
	currentTerm := c.CurrentTerm
	c.Mu.Unlock()

	c.ElecLogger.Printf("Election timeout started with duration %v, term=%d\n", duration, currentTerm)

	c.Mu.Lock()
	for c.getRole() == FOLLOWER {
		select {
		case <-timer:
			c.ElecLogger.Printf("Election timeout ended, checking if heartbeat received from leader\n")

			// check for timeout
			if time.Since(c.LastContact) < duration {
				timer, duration = newElectionTimer(TIMEOUT * time.Second)
				c.ElecLogger.Printf("Heartbeat received, new election timer started with duration %v, term=%d\n", duration, currentTerm)
				continue
			} else {
				c.ElecLogger.Printf("Heartbeat wasn't received on term=%d, promoting self to CANDIDATE\n", currentTerm)
				c.CurrentLeader.clearLeader() 
				c.CurrentRole = CANDIDATE
				// c.Mu.Unlock()
			}
		}
	}
	c.Mu.Unlock()
}

func (c *ClientInfo) candidate() {
	c.ElecLogger.Printf("State: CANDIDATE")
	// start election
	c.Mu.Lock()
	c.CurrentTerm += 1
	tmpTerm := c.CurrentTerm
	c.ElecLogger.Printf("Beginning new election with term %d\n", c.CurrentTerm)
	c.Mu.Unlock()

	timer, duration := newElectionTimer(TIMEOUT * time.Second)
	c.LastContact = time.Now()
	c.ElecLogger.Printf("Election timeout started with duration %v, term=%d\n", duration, tmpTerm)

	c.startElection(tmpTerm)

	for c.getRole() == CANDIDATE {
		select {
		case <- timer:
			c.ElecLogger.Printf("Election timeout ended, checking if election has been decided")
		}
	}
}

func (c *ClientInfo) leader() {

}

// start election function
func (c *ClientInfo) startElection(term int) {
	// vote for ourself
	c.VotedFor = c.ClientName
	c.VotesReceived = append(c.VotesReceived, c.ClientName)
	
	// send RequestVote RPC to outbound connections - hold mutex lock here
	for {
		// HENRY TODO
	}
}

func (c *ClientInfo) getRole() Role {
	c.Mu.Lock()
	state := c.CurrentRole
	c.Mu.Unlock()

	return state
}

// TODO: Do all the setup work before starting RAFT (public/private keys)
func (c *ClientInfo) init() {

}

// helper method to read incoming messages from all the incoming connections
func (c *ClientInfo) recvIncomingMessages(connection net.Conn, clientName string) {
	reader := bufio.NewReader(connection)
	for {
		// TODO: receive incoming messages, identify the message type, demarshal into the appropriate data structure, and pass the struct to the appropriate channel for RAFT to handle
		bytes, err := reader.ReadBytes('\n')
		
		if err != nil {
			c.ConnLogger.Printf("Inbound connection from client %s has disconnected\n", clientName)
			// TODO: handle disconnect ... (don't change leader if dc'd was leader but modify the outgoing connections and break out of loop)
			break
		}

		// parse command identification
		id := strings.Split(string(bytes[:len(bytes) - 1]), DELIM)[0]
		c.ConnLogger.Printf("Received command [%s] from %s\n", strings.ToUpper(id), clientName)
		
		idInt, _ := strconv.Atoi(id)
		c.reqChan(Rpc(idInt), bytes)
	}
}

func (c *ClientInfo) reqChan(id Rpc, b []byte) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch id {
	// If CANDIDATE, respond to the RequestVote result. Otherwise, ignore the request
	case REQUESTVOTE_REPLY:
	var data RequestVoteResponse
	if c.CurrentRole == CANDIDATE {
		data.Demarshal(b)
		c.ReqVoteResponseChan <- data
	}
	// All roles must respond to the RequestVote request
	case REQUESTVOTE_REQ:
	var data RequestVoteRequest
	data.Demarshal(b)
	c.ReqVoteRequestChan <- data
	}
}

// if minTime = 5, election timer lasts between 5 and 10 seconds
func newElectionTimer(minTime time.Duration) (<-chan time.Time, time.Duration) {
	extra := time.Duration(rand.Int63()) % minTime
	return time.After(minTime + extra), minTime + extra
}