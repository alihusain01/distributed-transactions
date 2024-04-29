package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"encoding/gob"
	// "sync"
	// "strconv"
)

var connectedServers = make(map[string]Server)
var currentServer Server

var encoders = make(map[string]*gob.Encoder)
var decoders = make(map[string]*gob.Decoder)

type Server struct {
	Name string
	Host string
	Port string
	Conn net.Conn
}

type Transaction struct {
	MessageType string
	TargetServer  string
	TargetAccount string
	Amount  int
	ID      float64
}

func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		fmt.Println("You must provide exactly two arguments: a branch (A-E) and a config file.")
		os.Exit(1)
	}

	branch := args[0]
	configFile := args[1]

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Println("The specified config file does not exist.")
		os.Exit(1)
	}

	servers, err := readConfigFile(configFile)

	if err != nil {
		fmt.Println("Error reading config file:", err)
		return
	}

	for _, server := range servers {
		if server.Name == branch {
			currentServer = server
			break
		}
	}

	listener, err := net.Listen("tcp", ":"+currentServer.Port)
	if err != nil {
		fmt.Printf("Error starting server on port %s: %s\n", currentServer.Port, err)
		os.Exit(1)
	}
	fmt.Printf("Server started on port %s\n", currentServer.Port)

	go establishConnections(servers)
	handleIncomingConnections(listener, servers)

	fmt.Printf("Branch %s has been successfully connected to all servers.\n", branch)

	time.Sleep(3 * time.Second)
	handleIncomingConnections(listener, nil)
}

func readConfigFile(filename string) ([]Server, error) {
	var servers []Server
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 3 {
			server := Server{
				Name: parts[0],
				Host: parts[1],
				Port: parts[2],
			}
			servers = append(servers, server)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return servers, nil
}

func establishConnections(servers []Server) {
	for _, server := range servers {
		if _, ok := connectedServers[server.Name]; !ok {
			for {
				conn, err := net.Dial("tcp", server.Host+":"+server.Port)
				if err != nil {
					fmt.Printf("Error connecting to %s at %s on port %s: %s\n", server.Name, server.Host, server.Port, err)
				} else {
					connectedServers[server.Name] = Server{Name: server.Name, Host: server.Host, Port: server.Port, Conn: server.Conn}
					fmt.Printf("Connected to %s at %s on port %s\n", server.Name, server.Host, server.Port)
					encoders[server.Name] = gob.NewEncoder(conn)
					decoders[server.Name] = gob.NewDecoder(conn)
					break
				}
			}
		}
	}
}

func handleIncomingConnections(listener net.Listener, servers []Server) {
	if servers == nil {
		fmt.Println("Now listening on TCP port: " + currentServer.Port + "\n")
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Failed to connect to client with err: ", err)
			}
			go transactionFromClient(conn)
		}
	} else {
		count := 0
		for {
			conn, _ := listener.Accept()
			count++

			go transactionFromCoordinator(conn)

			if count >= len(servers) {
				break
			}
		}
	}
}

func transactionFromClient(conn net.Conn) {
	fmt.Println("Successfully connected to client. Awaiting transaction.")
	clientDecoder := gob.NewDecoder(conn)
	clientEncoder := gob.NewEncoder(conn)
	for {
		var transaction Transaction
		var response string

		err := clientDecoder.Decode(&transaction)
		if err != nil {
			fmt.Println("Failed to receive transaction from client: ", err)
			return
		}

		fmt.Println("Received transaction from Client", transaction)

		if transaction.MessageType == "COMMIT" {
			fmt.Println("Received COMMIT command from client. Committing transaction.")
		} else if transaction.TargetServer == currentServer.Name {
			// Coordinator is intended recipient of transaction
			response = handleTransaction(transaction)
		} else {
			// Send the transaction to the corresponding server and await response.
			serverEncoder := encoders[transaction.TargetServer]
			serverEncoder.Encode(transaction)

			serverDecoder := decoders[transaction.TargetServer]
			serverDecoder.Decode(&response)
			fmt.Println("Received response from corresponding server: ", response)
		}

		// Abort transaction
		if response == "ABORT" || response == "NOT FOUND, ABORTED" || response == "ABORTED" {
			fmt.Println("Server failed to handle transaction. Aborting and rolling back state.")
		}

		// Reply back to client
		clientEncoder.Encode(response)
	}
}

func transactionFromCoordinator(conn net.Conn) {
	serverDecoder := gob.NewDecoder(conn)
	serverEncoder := gob.NewEncoder(conn)
	for {
		var transaction Transaction
		err := serverDecoder.Decode(&transaction)

		if err != nil {
			fmt.Println("Failed to receive transaction from coordinator", err)
			return
		}

		fmt.Println("Received transaction from coordinator: ", transaction)
		response := handleTransaction(transaction)

		serverEncoder.Encode(response)
	}
}

func handleTransaction(transaction Transaction) string {
	return "ABORTED"
}
