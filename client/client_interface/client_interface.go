package client

import (
	"fmt"
	"net"
	"sync"

	// dictionary "example/users/client/dictionary"
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
	ProcessId     int64            // process ID identifier
	ClientName    string           // name identifier: A, B, C, D, E
	OutboundConns []ConnectionInfo // outbound connections to other servers
	InboundConns  []ConnectionInfo // inbound connections to other servers
	CurrentTerm   int
	CurrentIndex  int
	ReplicatedLog []LogEntry
	VotedFor      string
	CurrentRole   Role // 1: follower / 2: leader / 3: candidate
	CurrentLeader LeaderInfo
	VotesReceived []string
	Mu            sync.Mutex

	// TODO: StateMachine ReplicatedDictOfDicts
	Shutdown      []ConnectionInfo // stop sending messages to and ignores messages received from the connections
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
	ClientName string // A, B, C, D, E
	InboundConnection net.Conn
	OutboundConnection net.Conn
}

func (c *ClientInfo) String() string {
	return fmt.Sprintf("\n===== Client Info =====\n"+
		"ProcessID: %d\n"+
		"ClientName: %s\n"+
		"current term: %d\n"+
		"votedFor: %s\n"+
		"currentRole %d\n"+
		"currentLeader %s\n",
		c.ProcessId, c.ClientName, c.CurrentTerm, c.VotedFor, c.CurrentRole, c.CurrentLeader)
}
