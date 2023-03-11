package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	crypto "example/users/client/crypto"
	disk "example/users/client/disk"
	storage "example/users/client/storage"
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
	GET Action = iota // get value from dict
	PUT               // put new value onto dict
	CREATE            // create new dictionary
)

const (
	REQUESTVOTE_REQ Rpc = iota
	REQUESTVOTE_REPLY
	APPENDENTRIES_REQ
	APPENDENTRIES_REPLY
	COMMAND
)

const (
	DELIM = "*"
	TIMEOUT = 7
	MAJORITY = 3
	MSG_DELIM = '@'
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
	CurrentTerm   int                 // current leader term
	LastLogIndex  int				  // index of latest entry in the log
	LastLogTerm   int				  // term of the latest entry in the log
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

	// Communication channels for RPC
	ReqVoteRequestChan      chan RequestVoteRequest
	ReqVoteResponseChan     chan RequestVoteResponse
	AppendEntryRequestChan  chan AppendEntryRequest
	AppendEntryResponseChan chan AppendEntryResponse

	CommitChan				chan<- LogEntry // Channel that contains LogEntry structs to commit

	// Random number generator
	r             *rand.Rand
}

// PROTOCOL: [identifier:marshalled byte slice]
type Marshaller interface {
	Marshal() []byte
	Demarshal([]byte) 
}

// Struct representing a single entry on the log, will be written/read to/from the disk in byte format
type LogEntry struct {
	Index            int
	Term             int
	Committed        bool
	Action           Action
	LogGetCommand    LogGetCommand
	LogPutCommand 	 LogPutCommand
	LogCreateCommand LogCreateCommand
	CommandId        string
}

// === THESE STRUCTS ARE FOR LOG ONLY AND WON'T BE SENT OVER THE NETWORK ===
type LogGetCommand struct {
	DictionaryId string
	ClientId	 string // id of client who issued the command
	EncryptedKey []byte // encrypted with the dictionary's public key
}

type LogPutCommand struct {
	DictionaryId      string
	ClientId          string // id of client who issued the command
	EncryptedKeyValue []byte // encrypted with the dictionary's public key
}

type LogCreateCommand struct {
	DictionaryId          string
	MemberClientId        []string // A, B, C, etc.
	DictionaryPublicKey   []byte   // dictionary's unencrypted public key
	DictionaryPrivateKeys [][]byte // encrypted dictionary's private keys using MemberClientId's public keys (can only be decrypted using the right private key for each member)
}
// ==========================================================================

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

// === THESE STRUCTS ARE FOR SENDING OVER THE NETWORK FOR RAFT ALGORITHM ===
type RequestVoteRequest struct {
	CandidateName string
	CandidateTerm int
	LastLogIndex  int
	LastLogTerm   int
}
type RequestVoteResponse struct {
	NewTerm     int
	VoteGranted bool
	ClientName  string
}

type AppendEntryRequest struct {
	Term         int
	LeaderName   string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntryResponse struct {
	NewTerm int  // term of self, may be higher than the LEADER who sent the AE RPC
	Success bool // true if previous entries of FOLLOWER were all committed
}

// Raw commands represent commands sent by the clients over the network, before encryption occurs.
// Should map 1-1 to user input in the console
type RawCommand struct {
	Action       Action
	ClientIds    []string
	DictionaryId string
	Key          string
	Value        string
	CommandId    string // ID unique to each command so clients can distinguish if command has already been committed
}

// Represent responses sent to the requesting client after Command on leader is committed
type CommandResponse struct {
	Message string // will be printed on the console
}

// ===========================================================================

func (r *RequestVoteRequest) Marshal() []byte {
	bytes, err := json.Marshal(r)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal RequestVoteRequest struct: %+v\n", *r))
	}

	return bytes
}

func (r *RequestVoteRequest) Demarshal(b []byte) {
	json.Unmarshal(b, r)
}


func (r *RequestVoteResponse) Marshal() []byte {
	bytes, err := json.Marshal(r)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal RequestVoteResponse struct: %+v\n", *r))
	}

	return bytes
}

func (r *RequestVoteResponse) Demarshal(b []byte) {
	json.Unmarshal(b, r)
}

func (a *AppendEntryRequest) Marshal() []byte {
	bytes, err := json.Marshal(a)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal AppndEntryRequest struct: %v\n", *a))
	}

	return bytes
}

func (a *AppendEntryRequest) Demarshal(b []byte) {
	json.Unmarshal(b, a)
}

func (a *AppendEntryResponse) Marshal() []byte {
	bytes, err := json.Marshal(a)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal AppendEntryResponse struct: %+v\n", *a))
	}

	return bytes
}

func (a *AppendEntryResponse) Demarshal(b []byte) {
	json.Unmarshal(b, a)
}

func (l *LogEntry) Marshal() []byte {
	bytes, err := json.Marshal(l)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal LogEntry struct: %+v\n", *l))
	}

	return bytes
}

func (l *LogEntry) Demarshal(b []byte) {
	json.Unmarshal(b, l)
}

func (c *RawCommand) Marshal() []byte {
	bytes, err := json.Marshal(c)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal Command struct: %+v\n", *c))
	}

	return bytes
}

func (c *RawCommand) Demarshal(b []byte) {
	json.Unmarshal(b, c)
}

func (r *CommandResponse) Marshal() []byte {
	bytes, err := json.Marshal(r)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal Command response struct: %+v\n", *r))
	}

	return bytes
}

func (r *CommandResponse) Demarshal(b []byte) {
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

func (l *LeaderInfo) setLeader(leaderName string, outboundConnection net.Conn) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.NoLeader = false
	l.ClientName = leaderName
	l.OutboundConnection = outboundConnection
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
	c.LastLogIndex = -1
	c.LastLogTerm = 0
	c.ReplicatedLog = make([]LogEntry, 0)
	c.VotedFor = ""
	c.CurrentRole = FOLLOWER
	c.CurrentLeader = LeaderInfo{ false, leaderInfoMu, "", nil, nil }
	c.VotesReceived = make([]string, 0)
	c.Mu = clientMu
	c.LogStore = disk.LogStore{} // TODO
	c.Faillinks = make(map[string]net.Conn)
	c.DiskLogger = log.New(os.Stdout, "[FSM]", log.LstdFlags)
	c.ElecLogger = log.New(os.Stdout, "[ELEC/AE]", log.LstdFlags)
	c.ConnLogger = log.New(os.Stdout, "[CONN]", log.LstdFlags)
	c.Keys.New()

	c.ReqVoteRequestChan = make(chan RequestVoteRequest)
	c.ReqVoteResponseChan = make(chan RequestVoteResponse)
	c.AppendEntryRequestChan = make(chan AppendEntryRequest)
	c.AppendEntryResponseChan = make(chan AppendEntryResponse)

	// set up RNG
	generator := rand.NewSource(processID)
	c.r = rand.New(generator)
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
			c.leader()
		}
	}
}

// Prologue to run before becoming a follower
func (c *ClientInfo) setupFollower() int {
	c.Mu.Lock()
	currentTerm := c.CurrentTerm
	c.VotedFor = ""
	c.VotesReceived = make([]string, 0)
	c.Mu.Unlock()

	return currentTerm
}

func (c *ClientInfo) follower() {
	c.ElecLogger.Printf("State: FOLLOWER\n")
	// start election timer, reset election timeout
	timer, duration := newElectionTimer(c.r, TIMEOUT * time.Second)

	currentTerm := c.setupFollower()

	c.ElecLogger.Printf("Election timeout started with duration %v, term=%d\n", duration, currentTerm)

	for c.getRole() == FOLLOWER {
		select {
		case <- timer:
			c.ElecLogger.Printf("AppendEntry heartbeat wasn't received before timeout on term=%d, promoting self to CANDIDATE\n", currentTerm)
			c.setRole(CANDIDATE)
		// Resets timer only if vote is successfully granted to CANDIDATE
		case request := <- c.ReqVoteRequestChan:
			c.ElecLogger.Printf("RequestVote request received from CANDIDIATE %s for term=%d, our term=%d\n", request.CandidateName, request.CandidateTerm, currentTerm)

			var responseData RequestVoteResponse
			conn, _ := c.OutboundConns.Get(request.CandidateName)

			if request.CandidateTerm > currentTerm {
				// update current term
				c.Mu.Lock()
				c.CurrentTerm = request.CandidateTerm
				c.Mu.Unlock()

				currentTerm = c.setupFollower()
			}

			if request.CandidateTerm == currentTerm && (c.VotedFor == "" || c.VotedFor == request.CandidateName) {
				c.VotedFor = request.CandidateName
				responseData.VoteGranted = true
				
				timer, _ = newElectionTimer(c.r, TIMEOUT * time.Second)
				c.ElecLogger.Printf("Voted for CANDIDATE %s, election timeout reset with duration %v, term=%d\n", request.CandidateName, duration, currentTerm)
			} else {
				c.ElecLogger.Printf("Did not vote for CANDIDATE %s, election timeout not reset, term=%d\n", request.CandidateName, currentTerm)
				responseData.VoteGranted = false
			}

			responseData.NewTerm = currentTerm
			responseData.ClientName = c.ClientName

			if conn != nil {
				c.sendRPC(REQUESTVOTE_REPLY, &responseData, conn, fmt.Sprintf("RequestVote RPC response with data: %+v", responseData), request.CandidateName)
			} else {
				panic("Connection is broken for RequestVote RPC response")
			}
		// Respond to AppendEntry request and reset timer
		case request := <- c.AppendEntryRequestChan:
			timer, duration = newElectionTimer(c.r, TIMEOUT * time.Second)
			c.ElecLogger.Printf("AppendEntry request received, election timeout reset with duration %v, term=%d\n", duration, currentTerm)

			var responseData AppendEntryResponse
			responseData.Success = false
			conn, _ := c.OutboundConns.Get(request.LeaderName)

			// LEADER term is out of date
			if request.Term < currentTerm {
				c.ElecLogger.Printf("LEADER's term=%d is out of date (our term=%d), no action will be taken\n", request.Term, currentTerm)
				responseData.NewTerm = currentTerm
			// LEADER term matches our term
			} else if request.Term == currentTerm {
				// TODO: after we finish leader election
				c.ElecLogger.Printf("LEADER's term=%d is same as our term, we love our LEADER %s <3\n", request.Term, request.LeaderName)
				responseData.NewTerm = currentTerm
				responseData.Success = true
				
				c.CurrentLeader.setLeader(request.LeaderName, conn)
			// LEADER is more up to date than us
			} else {
				c.ElecLogger.Printf("LEADER's term=%d is more up-to-date than our term=%d, updating our term\n", request.Term, currentTerm)
				responseData.NewTerm = request.Term

				c.Mu.Lock()
				c.CurrentTerm = responseData.NewTerm
				c.Mu.Unlock()

				currentTerm = c.setupFollower()
				c.CurrentLeader.setLeader(request.LeaderName, conn)
			}

			if conn != nil {
				c.sendRPC(APPENDENTRIES_REPLY, &responseData, conn, fmt.Sprintf("AppendEntry RPC response with data: %+v", responseData), request.LeaderName)
			} else {
				// If failed, we don't have to send it over
				panic("Connection is broken for AppendEntry RPC response")
			}
		}
	}
}

func (c *ClientInfo) candidate() {
	c.ElecLogger.Printf("State: CANDIDATE\n")
	c.CurrentLeader.clearLeader()
	// prepare for new election
	prepareElection := func() (<-chan time.Time, int) {
		c.Mu.Lock()
		c.CurrentTerm += 1
		tmpTerm := c.CurrentTerm

		c.ElecLogger.Printf("Beginning new election with term %d\n", c.CurrentTerm)
		c.Mu.Unlock()

		timer, duration := newElectionTimer(c.r, TIMEOUT * time.Second)
		c.ElecLogger.Printf("Election timeout started with duration %v, term=%d\n", duration, tmpTerm)

		return timer, tmpTerm
	}

	timer, tmpTerm := prepareElection()
	c.startElection(tmpTerm)
	
	for c.getRole() == CANDIDATE {
		select {
		case <- timer:
			c.ElecLogger.Printf("Election timeout ended for term=%d, restarting election\n", tmpTerm)
			
			timer, tmpTerm = prepareElection()
			c.startElection(tmpTerm)
		// Another CANDIDATE is requesting an election, would be nice if we cared...
		case request := <- c.ReqVoteRequestChan:
			c.ElecLogger.Printf("RequestVote request received from CANDIDIATE %s for term=%d, our term=%d\n", request.CandidateName, request.CandidateTerm, tmpTerm)

			var responseData RequestVoteResponse
			conn, _ := c.OutboundConns.Get(request.CandidateName)

			if request.CandidateTerm > tmpTerm {
				// update current term, clear our own vote
				c.Mu.Lock()
				c.CurrentTerm = request.CandidateTerm
				c.Mu.Unlock()

				tmpTerm = c.setupFollower()
				c.setRole(FOLLOWER)
			}

			if request.CandidateTerm == tmpTerm && (c.VotedFor == "" || c.VotedFor == request.CandidateName) {
				c.VotedFor = request.CandidateName
				responseData.VoteGranted = true
				
				// timer, _ = newElectionTimer(c.r, TIMEOUT * time.Second)
				c.ElecLogger.Printf("Stepped down as CANDIDATE because our term is behind, voted for CANDIDATE %s, term=%d\n", request.CandidateName, tmpTerm)
			} else {
				c.ElecLogger.Printf("Remain as CANDIDATE, did not vote for CANDIDATE %s, term=%d\n", request.CandidateName, tmpTerm)
				responseData.VoteGranted = false
			}

			responseData.NewTerm = tmpTerm
			responseData.ClientName = c.ClientName

			if conn != nil {
				c.sendRPC(REQUESTVOTE_REPLY, &responseData, conn, fmt.Sprintf("RequestVote RPC response with data: %+v", responseData), request.CandidateName)
			} else {
				panic("Connection is broken for RequestVote RPC response")
			}
			// fmt.Println("Done sending RPC!")
		// Receive RPC requests, LEADER revived or someone else already won the election
		case request := <- c.AppendEntryRequestChan:
			c.ElecLogger.Printf("AppendEntry request received on term=%d\n", tmpTerm)

			var responseData AppendEntryResponse
			responseData.Success = false
			conn, _ := c.OutboundConns.Get(request.LeaderName)

			// LEADER term is out of date
			if request.Term < tmpTerm {
				c.ElecLogger.Printf("LEADER's term=%d is out of date (our term=%d), no action will be taken\n", request.Term, tmpTerm)
				responseData.NewTerm = tmpTerm
			// LEADER term matches our term, become FOLLOWER
			} else if request.Term == tmpTerm {
				c.ElecLogger.Printf("LEADER's term=%d is same as our term, stepping down\n", request.Term)
				responseData.NewTerm = tmpTerm
				responseData.Success = true
				
				c.CurrentLeader.setLeader(request.LeaderName, conn)
				c.setRole(FOLLOWER)
			// LEADER is more up to date than us, become FOLLOWER
			} else {
				c.ElecLogger.Printf("LEADER's term=%d is more up-to-date than our term=%d, stepping down\n", request.Term, tmpTerm)
				responseData.NewTerm = request.Term

				c.Mu.Lock()
				c.CurrentTerm = responseData.NewTerm
				tmpTerm = responseData.NewTerm
				c.Mu.Unlock()

				c.CurrentLeader.setLeader(request.LeaderName, conn)
				c.setRole(FOLLOWER)
			}

			if conn != nil {
				c.sendRPC(APPENDENTRIES_REPLY, &responseData, conn, fmt.Sprintf("AppendEntry RPC response with data: %+v", responseData), request.LeaderName)
			} else {
				// If failed, we don't have to send it over
				panic("Connection is broken for AppendEntry RPC response")
			}
		// Respond to vote from server
		case reply := <- c.ReqVoteResponseChan:
			c.ElecLogger.Printf("RequestVote RPC response received during current term=%d, client=%s\n", tmpTerm, reply.ClientName)

			newState := c.getRole()
			if newState != CANDIDATE {
				c.ElecLogger.Printf("State %s is no longer CANDIDATE (probably shouldn't happen)\n", newState.String())

				return 
			}

			if reply.NewTerm > tmpTerm {
				c.ElecLogger.Printf("Received term=%d is greater than current term=%d\n", reply.NewTerm, tmpTerm)

				c.Mu.Lock()
				c.CurrentTerm = reply.NewTerm
				c.Mu.Unlock()
				c.setRole(FOLLOWER)
			} else if reply.NewTerm == tmpTerm {
				c.ElecLogger.Printf("Received term=%d is equal to the current term=%d\n", reply.NewTerm, tmpTerm)

				// check if we receive a vote
				if reply.VoteGranted {
					c.ElecLogger.Printf("Vote received from client: %s\n", reply.ClientName)
					c.VotesReceived = append(c.VotesReceived, reply.ClientName)
					if len(c.VotesReceived) >= MAJORITY {
						c.ElecLogger.Printf("Majority vote received of %d, promoting to LEADER\n", MAJORITY)
						c.setRole(LEADER)
					}
				} else {
					c.ElecLogger.Printf("Vote not received from client: %s\n", reply.ClientName)
				}
			} else {
				c.ElecLogger.Printf("Reply's term=%d is less than current term=%d, do nothing.\n", reply.NewTerm, tmpTerm)
			}
		}
	}
}

func (c *ClientInfo) leader() {
	c.ElecLogger.Printf("State: LEADER\n")

	c.Mu.Lock()
	tmpTerm := c.CurrentTerm
	c.VotesReceived = make([]string, 0)
	// c.VotedFor = ""
	c.CurrentLeader.setLeader(c.ClientName, nil)
	c.Mu.Unlock()

	// start heartbeat timer to send AppendEntry RPC
	heartbeat := time.NewTicker((TIMEOUT / 3) * time.Second)
	defer heartbeat.Stop()

	for c.getRole() == LEADER {
		select {
		// send out AppendEntry RPC
		case <- heartbeat.C:
			c.sendHeartBeat(tmpTerm)
		case reply := <- c.AppendEntryResponseChan:
			c.ElecLogger.Printf("AppendEntry RPC response received for term=%d\n", tmpTerm)

			if reply.NewTerm > tmpTerm {
				c.ElecLogger.Printf("Received term=%d is higher than our term=%d, stepping down\n", reply.NewTerm, tmpTerm)
				
				c.Mu.Lock()
				c.CurrentTerm = reply.NewTerm
				c.Mu.Unlock()

				c.CurrentLeader.clearLeader() // c.CurrentLeader will be set eventually when AppendEntry RPC requests come in
				c.setRole(FOLLOWER)
			}
		case request := <- c.ReqVoteRequestChan:
			c.ElecLogger.Printf("RequestVote request received from CANDIDIATE %s for term=%d, our term=%d\n", request.CandidateName, request.CandidateTerm, tmpTerm)

			var responseData RequestVoteResponse
			conn, _ := c.OutboundConns.Get(request.CandidateName)

			if request.CandidateTerm > tmpTerm {
				// update current term, become FOLLOWER
				c.Mu.Lock()
				c.CurrentTerm = request.CandidateTerm
				c.Mu.Unlock()

				tmpTerm = c.setupFollower()
				c.setRole(FOLLOWER)
			}

			if request.CandidateTerm == tmpTerm && (c.VotedFor == "" || c.VotedFor == request.CandidateName) {
				c.VotedFor = request.CandidateName
				responseData.VoteGranted = true
				
				// timer, _ = newElectionTimer(c.r, TIMEOUT * time.Second)
				c.ElecLogger.Printf("Stepped down as LEADER because our term is behind, voted for CANDIDATE %s, term=%d\n", request.CandidateName, tmpTerm)
			} else {
				c.ElecLogger.Printf("Remain as LEADER, did not vote for CANDIDATE %s, term=%d\n", request.CandidateName, tmpTerm)
				responseData.VoteGranted = false
			}

			responseData.NewTerm = tmpTerm
			responseData.ClientName = c.ClientName

			if conn != nil {
				c.sendRPC(REQUESTVOTE_REPLY, &responseData, conn, fmt.Sprintf("RequestVote RPC response with data: %+v", responseData), request.CandidateName)
			} else {
				panic("Connection is broken for RequestVote RPC response")
			}
		case request := <- c.AppendEntryRequestChan:
			c.ElecLogger.Printf("AppendEntry request received on term=%d\n", tmpTerm)

			var responseData AppendEntryResponse
			responseData.Success = false
			conn, _ := c.OutboundConns.Get(request.LeaderName)

			// LEADER term is out of date (and other LEADER should step down)
			if request.Term < tmpTerm {
				c.ElecLogger.Printf("LEADER's term=%d is out of date (our term=%d), tell LEADER to step down\n", request.Term, tmpTerm)
				responseData.NewTerm = tmpTerm
			// LEADER term matches our term, become FOLLOWER
			} else if request.Term == tmpTerm {
				c.ElecLogger.Printf("LEADER's term=%d is same as our term, stepping down\n", request.Term)
				responseData.NewTerm = tmpTerm
				responseData.Success = true
				
				c.CurrentLeader.setLeader(request.LeaderName, conn)
				c.setRole(FOLLOWER)
			// LEADER is more up to date than us, become FOLLOWER
			} else {
				c.ElecLogger.Printf("LEADER's term=%d is more up-to-date than our term=%d, stepping down\n", request.Term, tmpTerm)
				responseData.NewTerm = request.Term

				c.Mu.Lock()
				c.CurrentTerm = responseData.NewTerm
				tmpTerm = responseData.NewTerm
				c.Mu.Unlock()

				c.CurrentLeader.setLeader(request.LeaderName, conn)
				c.setRole(FOLLOWER)
			}

			if conn != nil {
				c.sendRPC(APPENDENTRIES_REPLY, &responseData, conn, fmt.Sprintf("AppendEntry RPC response with data: %+v", responseData), request.LeaderName)
			} else {
				// If failed, we don't have to send it over
				panic("Connection is broken for AppendEntry RPC response")
			}
		}
	}
}

// Start election after becoming a CANDIDATE
func (c *ClientInfo) startElection(term int) {
	// vote for ourself
	c.Mu.Lock()
	c.VotedFor = c.ClientName
	c.VotesReceived = append(c.VotesReceived, c.ClientName)
	c.Mu.Unlock()

	// fmt.Println("before check range out keys")
	// send RequestVote RPC to outbound connections
	for _, clientName := range c.OutboundConns.Keys() {
		conn, exists := c.OutboundConns.Get(clientName)

		if exists {
			go func(connection net.Conn, clientName string){
				c.ElecLogger.Printf("Sending RequestVote RPC to client: %s\n", clientName)

				c.Mu.Lock()
				requestData := RequestVoteRequest {
					CandidateName: c.ClientName,
					CandidateTerm: term,
					LastLogIndex: c.LastLogIndex,
					LastLogTerm: c.LastLogTerm,
				}

				c.Mu.Unlock()

				// message := []byte(string(strconv.Itoa(int(REQUESTVOTE_REQ)) + DELIM))
				// // fmt.Println(string(strconv.Itoa(int(REQUESTVOTE_REQ)) + DELIM)
				// message = append(message, requestData.Marshal()...)
				// message = append(message, byte(MSG_DELIM))
				// c.writeToConnection(c.ClientName, connection, message, fmt.Sprintf("RequestVote RPC with data: %+v", requestData))

				c.sendRPC(REQUESTVOTE_REQ, &requestData, connection, fmt.Sprintf("RequestVote RPC with data: %+v", requestData), clientName)	
			}(conn, clientName)
		}
	}
}

func (c *ClientInfo) sendHeartBeat(term int) {
	for _, clientName := range c.OutboundConns.Keys() {
		conn, exists := c.OutboundConns.Get(clientName)

		if exists {
			go func(connection net.Conn, clientName string) {
				c.ElecLogger.Printf("Sending AppendEntry RPC to client: %s\n", clientName)

				c.Mu.Lock()

				prevLogIndex := c.LastLogIndex - 1
				var prevLogTerm, commitIndex int
				if prevLogIndex < 0 {
					prevLogTerm = 0
					commitIndex = -1
				} else {
					// fetch term from log
					prevLogTerm = c.ReplicatedLog[prevLogIndex].Term
					
					for idx, entry := range c.ReplicatedLog {
						if entry.Committed {
							commitIndex = idx
						} else {
							break
						}
					}
				}

				requestData := AppendEntryRequest {
					Term: term, 
					LeaderName: c.ClientName,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries: nil, // Entries being nil means heartbeat
					CommitIndex: commitIndex,
				}
				c.Mu.Unlock()

				// message := []byte(string(strconv.Itoa(int(APPENDENTRIES_REQ)) + DELIM))
				// message = append(message, requestData.Marshal()...)
				// message = append(message, byte(MSG_DELIM))

				// c.writeToConnection(c.ClientName, connection, message, fmt.Sprintf("AppendEntry RPC with data: %+v", requestData))
				c.sendRPC(APPENDENTRIES_REQ, &requestData, connection, fmt.Sprintf("AppendEntry RPC with data: %+v", requestData), clientName)
			}(conn, clientName)
		}
	}
}

func (c *ClientInfo) getRole() Role {
	c.Mu.Lock()
	state := c.CurrentRole
	c.Mu.Unlock()

	return state
}

func (c *ClientInfo) setRole(newRole Role) {
	c.Mu.Lock()
	c.CurrentRole = newRole
	c.Mu.Unlock()
}

// TODO: Do all the setup work before starting RAFT (public/private keys)
func (c *ClientInfo) init() {

}

// helper method to read incoming messages from all the incoming connections
func (c *ClientInfo) recvIncomingMessages(connection net.Conn, clientName string) {
	reader := bufio.NewReader(connection)
	for {
		// TODO: receive incoming messages, identify the message type, demarshal into the appropriate data structure, and pass the struct to the appropriate channel for RAFT to handle
		bytes, err := reader.ReadBytes(MSG_DELIM)
		
		if err != nil {
			c.ConnLogger.Printf("Inbound connection from client %s has disconnected\n", clientName)
			// TODO: handle disconnect ... (don't change leader if dc'd was leader but modify the outgoing connections and break out of loop)
			break
		}

		// parse command identification
		parse := strings.Split(string(bytes[:len(bytes) - 1]), DELIM)
		id := parse[0]
		data := parse[1]

		c.ConnLogger.Printf("Received command [%s] from %s with data [%s]\n", strings.ToUpper(id), clientName, data)
		
		idInt, _ := strconv.Atoi(id)
		c.reqChan(Rpc(idInt), []byte(data))
	}
}

func (c *ClientInfo) reqChan(id Rpc, b []byte) {
	switch id {
	// If CANDIDATE, respond to the RequestVote result. Otherwise, ignore the request
	case REQUESTVOTE_REPLY:
		var data RequestVoteResponse
		if c.getRole() == CANDIDATE {
			data.Demarshal(b)
			c.ReqVoteResponseChan <- data
		}
	// All roles must respond to the RequestVote request
	case REQUESTVOTE_REQ:
		var data RequestVoteRequest
		data.Demarshal(b)
		c.ReqVoteRequestChan <- data
	// Only the LEADER must answer the AppendEntry result
	case APPENDENTRIES_REPLY:
		var data AppendEntryResponse
		if c.getRole() == LEADER {
			data.Demarshal(b)
			c.AppendEntryResponseChan <- data
		}
	// All roles must respond to the AppendEntry request
	case APPENDENTRIES_REQ:
		var data AppendEntryRequest
		data.Demarshal(b)
		c.AppendEntryRequestChan <- data
	// Only the LEADER processes the command and adds it to its log, if valid
	case COMMAND:
		var data RawCommand
		
		if c.getRole() == LEADER {
			data.Demarshal(b)
			c.submit(data)
		} else {
			c.ElecLogger.Printf("Client isn't leader, command ignored\n")
		}
	}
}

// if minTime = 5, election timer lasts between 5 and 10 seconds
func newElectionTimer(rand *rand.Rand, minTime time.Duration) (<-chan time.Time, time.Duration) {
	extra := time.Duration(rand.Int63()) % minTime
	return time.After(minTime + extra), minTime + extra
}

func (c *ClientInfo) sendRPC(reqId Rpc, data Marshaller, connection net.Conn, errMsg string, outBoundClientName string) {
	message := []byte(string(strconv.Itoa(int(reqId)) + DELIM))
	message = append(message, data.Marshal()...)
	message = append(message, byte(MSG_DELIM))

	go c.writeToConnection(outBoundClientName, connection, message, errMsg)
}

// Write a message to the passed connection object
func (c *ClientInfo) writeToConnection(clientName string, connection net.Conn, message []byte, errMessage string) {
	time.Sleep(3 * time.Second)
	_, err := connection.Write(message)

	c.handleWriteError(err, errMessage, connection, clientName)
}

// Process write error [incomplete]
func (c *ClientInfo) handleWriteError(err error, errMessage string, connection net.Conn, clientName string) {
	if err != nil {
		c.ConnLogger.Printf("err=%s, client=%s, [INFO: %s]\n", err.Error(), clientName, errMessage)
		connection.Close()

		c.OutboundConns.Set(clientName, nil)
	}
}

// Submits the command to the log if client is the leader
func (c *ClientInfo) submit(command RawCommand) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.CurrentRole == LEADER {
		logEntry := c.handleRawCommand(command)

		c.ReplicatedLog = append(c.ReplicatedLog, logEntry)
		c.ElecLogger.Printf("LogEntry %+v saved locally with term=%d and previous index=%d\n", c.CurrentTerm, c.LastLogIndex)

		// TODO: persist log on disk
	}

	return
}

// This function processes the command and generates the log entry. ASSUMPTION: happens under a mutex lock
func (c *ClientInfo) handleRawCommand(command RawCommand) LogEntry {
	// TODO: IAN - handle command, create a LogEntry struct and return it
	return LogEntry{}
}