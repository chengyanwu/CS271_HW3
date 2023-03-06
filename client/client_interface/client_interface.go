package client

import (
	"fmt"
	"net"
	"sync"
	"log"
	"os"
	"bufio"
	"strings"

	storage "example/users/client/storage"
	disk   "example/users/client/disk"
	crypto "example/users/client/crypto"
)

type Role uint8
type Action uint8

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
	DELIM = ":"
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

// Check if current client is the leader
func (c *ClientInfo) CheckSelf() bool {
	return c.CurrentLeader.ClientName == c.ClientName
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
	c.Keys.New()
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
	// for {
		// TODO: main RAFT loop
		// switch statement for leader, candidiate, and follower
	//}
}

// Do all the setup work before starting RAFT (public/private keys)
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
			// TODO: handle disconnect ... (don't change leader but modify the outgoing connections and break out of loop)
			break
		}

		// parse command identification
		id := strings.Split(string(bytes[:len(bytes) - 1]), DELIM)[0]
		c.ConnLogger.Printf("Received command [%s] from %s\n", strings.ToUpper(id), clientName)
	}
}