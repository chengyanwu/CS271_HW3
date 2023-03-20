package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"log"
	"time"

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
var commitChan chan string

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
	
	takeUserInput()
}

// init server with default configuration and connect to other servers
func serverInit() {
	commitChan = make(chan string, 1000)

	// Creates a new RAFT and perform recovery
	myInfo.NewRaft(processId, os.Args[1], &clientInfoMu, &leaderInfoMutex, commitChan)

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

// Establish outbound connection to the specific port, return nil if connection doesn't exist
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
	// var action string
	commandIndex := len(myInfo.ReplicatedLog)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("===== Actions =====\np - print client info\ncreate - creates a new dictionary with given members (A-E)\nget - get a key's value from dictionary with given id\nput - add a key-value pair to dictionary with given id\nprintDict - prints out information for dictionary with given id\nprintAll - print out the ids of all dictionaries we are a member of\nfailLink - fails a link between self and another process\nfixLink - fixes a link between self and another process with an existing failed link\nfailProcess - fails our process\n===================")
	for {
		var command client.RawCommand
		action, err := reader.ReadString('\n')
		action = action[:len(action) - 1] // removes delimiter byte
		split := strings.Split(action, " ")
		
		if err != nil {
			fmt.Println("Error occurred when scanning input")
			continue
		} else if split[0] == "p" {
			fmt.Println(&myInfo)
			continue
		} else if split[0] == "create" {
			// parse client IDs
			clientIDs := split[1:]

			if len(clientIDs) < 1 {
				fmt.Println("USAGE: create <client-id> <client_id>...")
				continue
			}

			invalid := false 
			for _, id := range clientIDs {
				if !(id == "A" || id == "B" || id == "C" || id == "D" || id == "E") {
					fmt.Println("Invalid ID:", id)
					invalid = true
				}
			}

			if invalid {
				continue
			}

			command = client.RawCommand{
				Action: client.CREATE,
				SenderName: myInfo.ClientName,
				CommandId: fmt.Sprintf("%s:%d", myInfo.ClientName, commandIndex),
				ClientIds: clientIDs,
			}

			commandIndex++
		} else if split[0] == "get" {
			if len(split) < 3 {
				fmt.Println("USAGE: get <dictionary_id> <key>")
				continue
			}

			dictionaryId := split[1]
			key := split[2]

			// check if dictionary with dictionaryId exists
			if !myInfo.StateMachine.Exists(dictionaryId) {
				fmt.Printf("DictionaryId %s doesn't exist\n", dictionaryId)
				continue
			}

			command = client.RawCommand{
				Action: client.GET,
				SenderName: myInfo.ClientName,
				CommandId: fmt.Sprintf("%s:%d", myInfo.ClientName, commandIndex),
				DictionaryId: dictionaryId,
				Key: key,
			}
			commandIndex++
		} else if split[0] == "put" {
			if len(split) < 4 {
				fmt.Println("USAGE: put <dictionary_id> <key> <value>")
				continue
			}

			dictionaryId := split[1]
			key := split[2]
			value := split[3]
			
			// check if dictionary with dictionaryId exists
			if !myInfo.StateMachine.Exists(dictionaryId) {
				fmt.Printf("DictionaryId %s doesn't exist\n", dictionaryId)
				continue
			}
			
			command = client.RawCommand{
				Action: client.PUT,
				SenderName: myInfo.ClientName,
				CommandId: fmt.Sprintf("%s:%d", myInfo.ClientName, commandIndex),
				DictionaryId: dictionaryId,
				Key: key,
				Value: value,
			}
			commandIndex++
		} else if split[0] == "printDict" {
			if len(split) < 2 {
				fmt.Println("USAGE: printDict <dictionary_id>")
				continue
			}

			dictionaryId := split[1]
			clientIds, exists := myInfo.StateMachine.GetClientIds(dictionaryId)

			if exists {
				dictContent, _ := myInfo.StateMachine.PrintDict(dictionaryId)

				fmt.Println("===================================")
				fmt.Printf("Client IDs: %+v\n", clientIds)
				fmt.Printf("Dict Content:\n%+v", dictContent)
				fmt.Println("===================================")
			} else {
				fmt.Println("=====================================")
				fmt.Println("No dictionary found with the given ID")
				fmt.Println("=====================================")
			}
			continue
		} else if split[0] == "printAll" {
			fmt.Println("====================================")
			// return all the member dictionaries of the current state machine
			memberDicts := myInfo.StateMachine.GetMemberDictionaries(myInfo.ClientName) 
			if len(memberDicts) > 0 {
				fmt.Println("Members: ", memberDicts)
			} else {
				fmt.Println("There are no dictionaries that we are a member of")
			}
			fmt.Println("====================================")	
			continue
		} else if split[0] == "failLink" {
			if len(split) < 2 {
				fmt.Println("USAGE: failLink <A-E>")
			}

			link := split[1]

			if link != myInfo.ClientName {
				myInfo.Faillinks.Set(link, true)
			} else {
				fmt.Println("Can't fail ourself :(")
			}
			continue
		} else if split[0] == "fixLink" {
			if len(split) < 2 {
				fmt.Println("USAGE: fixLink <A-E>")
			}

			link := split[1]

			if link != myInfo.ClientName {
				myInfo.Faillinks.Set(link, false)
			} else {
				fmt.Println("Can't fix ourself :(")
			}
			continue
		} else if split[0] == "failProcess" {
			myInfo.DiskStore.CloseFDs()
			os.Exit(0)
			continue
		} else {
			fmt.Println("Invalid action:", action)
			continue
		}

		sendCommand(command)
		timer := time.After(15 * time.Second)

		// Wait for the command to be committed, or timeout
		for commit := false; !commit; {
			// fmt.Println("In loop")
			select {
			case id := <- commitChan:
				if id == command.CommandId {
					commit = true
				}
			case <- timer:
				// timeout, send request again
				sendCommand(command)
				timer = time.After(15 * time.Second)
			}
		}
		fmt.Println("Past loop")
	}
}

func sendCommand(command client.RawCommand) {
	if myInfo.CheckSelf() {
		// We are LEADER, just submit to our log
		go myInfo.Submit(command) 
	} else {
		myInfo.CurrentLeader.Mu.Lock()
		conn := myInfo.CurrentLeader.OutboundConnection
		name := myInfo.CurrentLeader.ClientName
		myInfo.CurrentLeader.Mu.Unlock()

		if conn != nil {
			go myInfo.SendRPC(client.COMMAND, &command, conn, "Command send failed, no LEADER yet or LEADER has crashed, will retry\n", name)
		} else {
			fmt.Println("Leader not established, retrying after 15 seconds.")
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
	_, err := connection.Write([]byte(message))

	handleError(err, "Error writing.", connection)
}
