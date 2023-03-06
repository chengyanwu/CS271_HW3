package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"log"

	// "errors"
	// "io"
	"sync"
	"bufio"

	client "example/users/client/client_interface"
)

const (
	SERVER_HOST = "localhost"
	SERVER_TYPE = "tcp"

	A = "6000"
	B = "6001"
	C = "6002"
	D = "6003"
	E = "6004"
)

var myInfo client.ClientInfo
var port string
var processId int64
var clientInfoMu sync.Mutex
var leaderInfoMutex sync.Mutex

func main() {
	processId = int64(os.Getpid())
	fmt.Println("My process ID:", processId)
	
	myInfo.ProcessId = processId

	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go [client name (A-E)] ")
		os.Exit(1)
	}

	serverInit()

	// start running raft 
	go myInfo.RAFT()
	// go myInfo.FSM()
	
	go takeUserInput()
}

// init server with default configuration and connect to other servers
func serverInit() {
	myInfo.NewRaft(processId, os.Args[1], &clientInfoMu, &leaderInfoMutex)

	if myInfo.ClientName == "A" {
		port = A
	} else if myInfo.ClientName == "B" {
		port = B
	} else if myInfo.ClientName == "C" {
		port = C
	} else if myInfo.ClientName == "D" {
		port = D
	} else {
		port = E
	}

	go startServer(port, myInfo.ClientName) // listen to incoming connections and connect to them!

	// wait for all clients to be set up, then connect to them
	// fmt.Println("Press \"enter\" AFTER all clients' servers are set up to connect to them")
	// fmt.Scanln()

	if myInfo.ClientName == "A" {
		myInfo.OutboundConns.Set("B", establishConnection(B))
		myInfo.OutboundConns.Set("C", establishConnection(C))
		myInfo.OutboundConns.Set("D", establishConnection(D))
		myInfo.OutboundConns.Set("E", establishConnection(E))
	} else if myInfo.ClientName == "B" {
		myInfo.OutboundConns.Set("A", establishConnection(A))
		myInfo.OutboundConns.Set("C", establishConnection(C))
		myInfo.OutboundConns.Set("D", establishConnection(D))
		myInfo.OutboundConns.Set("E", establishConnection(E))
	} else if myInfo.ClientName == "C" {
		myInfo.OutboundConns.Set("A", establishConnection(A))
		myInfo.OutboundConns.Set("B", establishConnection(B))
		myInfo.OutboundConns.Set("D", establishConnection(D))
		myInfo.OutboundConns.Set("E", establishConnection(E))
	} else if myInfo.ClientName == "D" {
		myInfo.OutboundConns.Set("A", establishConnection(A))
		myInfo.OutboundConns.Set("B", establishConnection(B))
		myInfo.OutboundConns.Set("C", establishConnection(C))
		myInfo.OutboundConns.Set("E", establishConnection(E))
	} else if myInfo.ClientName == "E" {
		myInfo.OutboundConns.Set("A", establishConnection(A))
		myInfo.OutboundConns.Set("B", establishConnection(B))
		myInfo.OutboundConns.Set("C", establishConnection(C))
		myInfo.OutboundConns.Set("D", establishConnection(D))
	}

	fmt.Println("Press \"enter\" AFTER all connections are established to begin RAFT elections!")
	fmt.Scanln()	
}

func startServer(port, name string) {
	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+port)

	if err != nil {
		log.Fatalf("Error starting server: %s\n", err.Error())
		// fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err.Error())

		// os.Exit(1)
	}

	defer server.Close()
	fmt.Println("Listening on " + SERVER_HOST + ":" + port)

	for {
		// Listen for inbound connection
		inboundConn, err := server.Accept()
		handleError(err, "Error accepting client.", inboundConn)

		// PROTOCOL: write [self name]
		writeToConnection(inboundConn, name+"\n")

		// PROTOCOL: receive [incoming client name, client port number]
		clientName, err := bufio.NewReader(inboundConn).ReadBytes('\n')
		handleError(err, "Didn't receive connected client's name.", inboundConn)

		nameSlice := strings.Split(string(clientName[:len(clientName)-1]), ":")

		go setupInboundChannel(inboundConn, nameSlice[0], nameSlice[1])
	}
}

// Add incoming connection to map and establish outbound connection if doesn't yet exist.
func setupInboundChannel(connection net.Conn, clientName string, clientPort string) {
	// fmt.Printf("Inbound client %s connected\n", clientName)
	myInfo.InboundConns.Set(clientName, connection)
	myInfo.Listen(connection, clientName) // start receiving messages on the inbound connection

	// set up outbound connection
	if value, exists := myInfo.OutboundConns.Get(clientName); exists && value == nil {
		connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+clientPort)
		handleError(err, fmt.Sprintf("Failed to RESPONSE connect to client with name: %s\n", clientName), connection)

		_, err = bufio.NewReader(connection).ReadBytes('\n')
		handleError(err, fmt.Sprintf("Didn't receive client's response on port: %s\n", clientPort), connection)
		fmt.Println("Successfully established outbound channel to client with name:", clientName)

		writeToConnection(connection, myInfo.ClientName+":"+port+"\n")
		
		myInfo.OutboundConns.Set(clientName, connection)
	}
}

// Establish outbound connection to the specific port
func establishConnection(clientPort string) net.Conn {
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+clientPort)

	if err != nil {
		return nil
	}

	// PROTOCOL: receive [client name]
	clientName, err := bufio.NewReader(connection).ReadBytes('\n')
	handleError(err, fmt.Sprintf("Didn't receive client's response on port: %s\n", clientPort), connection)
	fmt.Println("Successfully established outbound channel to client with name:", string(clientName[:len(clientName) - 1]))
	
	// PROTOCOL: send self name and port number to connection
	writeToConnection(connection, myInfo.ClientName+":"+port+"\n")

	return connection
}

func takeUserInput() {
	var action string

	fmt.Println("===== Actions =====\np - print client info\n===================")
	for {
		_, err := fmt.Scanln(&action)

		if err != nil {
			fmt.Println("Error occurred when scanning input")
		} else if action == "p" {
			fmt.Println(&myInfo)
		} else if action == "s" {
			// TODO: Ian
		} else {
			fmt.Println("Invalid action:", action)
		}
	}
}

func handleError(err error, message string, connection net.Conn) {
	if err != nil {
		fmt.Println(message, err.Error())
		connection.Close()

		os.Exit(1)
	}
}

func writeToConnection(connection net.Conn, message string) {
	time.Sleep(3 * time.Second)
	_, err := connection.Write([]byte(message))

	handleError(err, "Error writing.", connection)
}
