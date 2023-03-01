package client

import (
	"fmt"
	"net"
)

type Role int64

const (
	FOLLOWER Role = 0
	LEADER   Role = 1
	CANDI    Role = 2
)

type ClientInfo struct {
	ProcessId     int64            // process ID identifier
	ClientName    string           // name identifier
	Conns         []ConnectionInfo // connections to other servers
	CurrentTerm   int
	VotedFor      string
	CurrentRole   Role // 1: follower / 2: leader / 3: candidate
	CurrentLeader string
	VotesReceived []string
}

type ConnectionInfo struct {
	Connection net.Conn
	ClientName string
}

func (c ClientInfo) String() string {
	return fmt.Sprintf("\n===== Client Info =====\n"+
		"ProcessID: %d\n"+
		"ClientName: %s\n"+
		"current term: %d\n"+
		"votedFor: %s\n"+
		"currentRole %d\n"+
		"currentLeader %s\n",
		c.ProcessId, c.ClientName, c.CurrentTerm, c.VotedFor, c.CurrentRole, c.CurrentLeader)
}
