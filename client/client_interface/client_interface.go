package client

import (
	"fmt"
	"net"
	"sync"
	"log"
	"os"

	// dictionary "example/users/client/dictionary"
	disk   "example/users/client/disk"
	crypto "example/users/client/crypto"
)

type Role uint8
type Action uint8

const (
	FOLLOWER Role = iota
	LEADER
	CANDIDATE
)

const (
	GET Action = iota
	PUT
	DELETE
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
	ProcessId     int64               // process ID identifier
	ClientName    string              // name identifier: A, B, C, D, E
	OutboundConns map[string]net.Conn // outbound connections to other servers
	InboundConns  map[string]net.Conn // inbound connections from other servers
	CurrentTerm   int
	CurrentIndex  int
	ReplicatedLog []LogEntry          // log entries, read from the disk
	VotedFor      string              // name of client voted for leader
	CurrentRole   Role                // 1: follower / 2: leader / 3: candidate
	CurrentLeader LeaderInfo	      // information of the current leader
	VotesReceived []string            // names of channels whose vote were received
	Mu            *sync.Mutex
	LogStore      disk.LogStore        // interact to store logs on disk

	// TODO: StateMachine ReplicatedDictOfDicts
	Faillinks     map[string]net.Conn // Stop sending messages to and ignores messages received from the connections here
	Keys		  crypto.Keys         // public and private keys
	DiskLogger	  *log.Logger         // logs information about AppendEntry RPCs and commits to the state machine
	ElecLogger    *log.Logger		  // logs information about Election RPCs and leader changes
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

// Check if current client is the leader
func (c *ClientInfo) CheckSelf() bool {
	return c.CurrentLeader.ClientName == c.ClientName
}

func (c *ClientInfo) String() string {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	outKeys := make([]string, 0, len(c.OutboundConns))
    for k := range c.OutboundConns {
        outKeys = append(outKeys, k)
    }

	inKeys := make([]string, 0, len(c.InboundConns))
	for k := range c.InboundConns {
		inKeys = append(inKeys, k)
	}

	return fmt.Sprintf("\n===== Client Info =====\n"+
		"Process ID: %d\n"+
		"Client name: %s\n"+
		"Current term: %d\n"+
		"Voted for: %s\n"+
		"Current role: %s\n"+
		"Leader: %s\n"+
		"Outbound connections: %+v\n"+
		"Inbound connections: %+v\n",
		c.ProcessId, c.ClientName, c.CurrentTerm, c.VotedFor, c.CurrentRole.String(), c.CurrentLeader.ClientName, outKeys, inKeys)
}

// Sets up client info upon first initialization of RAFT algorithm
func (c *ClientInfo) NewRaft(processID int64, name string, clientMu *sync.Mutex, leaderInfoMu *sync.Mutex) {
	c.ProcessId = processID
	c.ClientName = name
	c.OutboundConns = make(map[string]net.Conn)
	c.InboundConns = make(map[string]net.Conn)
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
	c.DiskLogger = log.New(os.Stdout, "[LOG]", log.LstdFlags)
	c.ElecLogger = log.New(os.Stdout, "[ELECTION]", log.LstdFlags)
	c.Keys.New()
}

// Recover client information after crashing
func (c *ClientInfo) Recover() {
	// TODO
}
