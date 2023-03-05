package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	// "errors"
	// "io"
	// "sync"

	"bufio"
	"math/rand"

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
var generator = rand.NewSource(myInfo.ProcessId)
var r = rand.New(generator)
var serverList = []string{A, B, C, D, E}

func main() {
	processId := int64(os.Getpid())
	fmt.Println("My process ID:", processId)
	myInfo.ProcessId = processId

	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go [client name (A-E)] ")
		os.Exit(1)
	}

	serverInit()
}

// init server with default configuration and connect to other servers
func serverInit() {
	myInfo.ClientName = os.Args[1]
	myInfo.CurrentTerm = 0
	myInfo.VotedFor = ""
	myInfo.CurrentRole = client.FOLLOWER
	myInfo.CurrentLeader = ""
	myInfo.VotesReceived = []string{}

	go startServer()

	// wait for all clients to be set up
	fmt.Println("Press \"enter\" AFTER all clients' servers are set up to connect to them")
	fmt.Scanln()

	if myInfo.ClientName == "A" {
		port = A
		myInfo.Conns = []client.ConnectionInfo{
			{ClientName: "A", Connection: establishConnection(B)},
			{ClientName: "B", Connection: establishConnection(C)},
			{ClientName: "C", Connection: establishConnection(D)},
			{ClientName: "D", Connection: establishConnection(E)},
		}
	} else if myInfo.ClientName == "B" {
		port = B
		myInfo.Conns = []client.ConnectionInfo{
			{ClientName: "A", Connection: establishConnection(B)},
			{ClientName: "C", Connection: establishConnection(C)},
			{ClientName: "D", Connection: establishConnection(D)},
			{ClientName: "E", Connection: establishConnection(E)},
		}
	} else if myInfo.ClientName == "C" {
		port = C
		myInfo.Conns = []client.ConnectionInfo{
			{ClientName: "A", Connection: establishConnection(B)},
			{ClientName: "B", Connection: establishConnection(C)},
			{ClientName: "D", Connection: establishConnection(D)},
			{ClientName: "E", Connection: establishConnection(E)},
		}
	} else if myInfo.ClientName == "D" {
		port = D
		myInfo.Conns = []client.ConnectionInfo{
			{ClientName: "A", Connection: establishConnection(B)},
			{ClientName: "B", Connection: establishConnection(C)},
			{ClientName: "C", Connection: establishConnection(D)},
			{ClientName: "E", Connection: establishConnection(E)},
		}
	} else if myInfo.ClientName == "E" {
		port = E
		myInfo.Conns = []client.ConnectionInfo{
			{ClientName: "A", Connection: establishConnection(B)},
			{ClientName: "B", Connection: establishConnection(C)},
			{ClientName: "C", Connection: establishConnection(D)},
			{ClientName: "D", Connection: establishConnection(E)},
		}
	}

}

func startServer() {

	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+port)
	if err != nil {
		fmt.Println("Error starting server:", err.Error())

		os.Exit(1)
	}

	defer server.Close()
	fmt.Println("Listening on " + SERVER_HOST + ":" + port)

	for {
		// inbound connection
		inboundChannel, err := server.Accept()
		handleError(err, "Error accepting client.", inboundChannel)

		// PROTOCOL: broadcast self name to connection
		writeToConnection(inboundChannel, myInfo.ClientName+"\n")

		// PROTOCOL: receive inbound client name
		clientName, err := bufio.NewReader(inboundChannel).ReadBytes('\n')
		handleError(err, "Didn't receive connected client's name.", inboundChannel)

		nameSlice := strings.Split(string(clientName[:len(clientName)-1]), ":")

		go processInboundChannel(inboundChannel, nameSlice[0])

	}
}

func processInboundChannel(connection net.Conn, clientName string) {
	// TODO
}

// connecting to server
func establishConnection(serverPort string) net.Conn {
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+serverPort)

	defer connection.Close()

	handleError(err, fmt.Sprintf("Couldn't connect to client's server with port: %s\n", port), connection)

	return connection
}

func takeUserInput() {
	fmt.Println("All outbound connections established")
	var action string

	fmt.Println("===== Actions =====\np - print client info\n===================")
	for {
		_, err := fmt.Scanln(&action)

		if err != nil {
			fmt.Println("Error occurred when scanning input")
		} else if action == "p" {
			fmt.Println(myInfo)
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
	// time.Sleep(3 * time.Second)
	_, err := connection.Write([]byte(message))

	handleError(err, "Error writing.", connection)
}
