package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var connectedServers = make(map[string]Server)
var currentServer Server

var decoders = make(map[string]*gob.Decoder)
var encoders = make(map[string]*gob.Encoder)

var accounts = make(map[string]int) // List of accounts and their balances.

var readTimestamps = make(map[string]float64)                     // List of transaction ids (timestamps) that have read the committed value.
var latestWriteTimeStamp = make(map[string]float64)               // List of transaction ids (timestamps) that have written the committed value.
var tentativeWriteTimestamps = make(map[string][]TentativeWrites) // List of tentative writes sorted by the corresponding transaction ids (timestamps)

type Server struct {
	Name string
	Host string
	Port string
	Conn net.Conn
}

type TentativeWrites struct {
	transactionID float64
	Balance       int
	IsCommitted   bool
}

type Transaction struct {
	MessageType   string
	TargetServer  string
	TargetAccount string
	Amount        int
	ID            float64
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
					// fmt.Printf("Error connecting to %s at %s on port %s: %s\n", server.Name, server.Host, server.Port, err)
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
			//TODO: Commit transaction
			fmt.Println("Received COMMIT command from client. Committing transaction.")
		} else if transaction.TargetServer == currentServer.Name {
			response = transactionHandler(transaction)
		} else {
			serverEncoder := encoders[transaction.TargetServer]
			serverEncoder.Encode(transaction)

			serverDecoder := decoders[transaction.TargetServer]
			serverDecoder.Decode(&response)
			fmt.Println("Received response from corresponding server: ", response)
		}

		if response == "ABORT" || response == "NOT FOUND, ABORTED" || response == "ABORTED" {
			fmt.Println("Server failed to handle transaction. Aborting and rolling back state.")
		}

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
		response := transactionHandler(transaction)

		serverEncoder.Encode(response)
	}
}

func transactionHandler(transaction Transaction) string {
	if transaction.MessageType == "DEPOSIT" {
		return handleDeposit(transaction)
	} else if transaction.MessageType == "WITHDRAW" {
		return handleWithdraw(transaction)
	} else if transaction.MessageType == "BALANCE" {
		return handleBalance(transaction)
	}
	//  else if transaction.MessageType == "COMMIT" {
	// 	//TODO: Commit transaction
	// 	return "OK"
	// } else {
	// 	return "ABORTED"
	// }
	return "ABORTED"
}

func handleDeposit(transaction Transaction) string {
	var final_balance int
	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
			if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID <= transaction.ID {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
					final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					if transaction.ID > readTimestamps[transaction.TargetAccount] {
						readTimestamps[transaction.TargetAccount] = transaction.ID
					}
				} else {
					if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID == transaction.ID {
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					} else {
						// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
						return handleDeposit(transaction)
					}
				}
			}
		}

		if _, ok := accounts[transaction.TargetAccount]; ok {
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID >= latestWriteTimeStamp[transaction.TargetAccount] {
				//Perform a tentative write on D: If Tc already has an entry in the TW list for D, update it. Else, add Tc and its write value to the TW list.
				if _, alright := tentativeWriteTimestamps[transaction.TargetAccount]; !alright {
					tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, final_balance + transaction.Amount, false})
				} else {
					for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID == transaction.ID {
							tentativeWriteTimestamps[transaction.TargetAccount][i].Balance = final_balance + transaction.Amount
							break
						}
					}
				}
				return "OK"
			} else {
				return "ABORTED"
			}
		} else {
			accounts[transaction.TargetAccount] = 0
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, transaction.Amount, false})
				return "OK"
			} else {
				return "ABORTED"
			}
		}
	} else {
		return "ABORTED"
	}
}

func handleWithdraw(transaction Transaction) string {
	var final_balance int
	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
			if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID <= transaction.ID {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
					final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					if transaction.ID > readTimestamps[transaction.TargetAccount] {
						readTimestamps[transaction.TargetAccount] = transaction.ID
					}
				} else {
					if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID == transaction.ID {
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					} else {
						// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
						return handleWithdraw(transaction)
					}
				}
			}
		}

		if _, ok := accounts[transaction.TargetAccount]; ok {
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID >= latestWriteTimeStamp[transaction.TargetAccount] {
				//Perform a tentative write on D: If Tc already has an entry in the TW list for D, update it. Else, add Tc and its write value to the TW list.
				if _, alright := tentativeWriteTimestamps[transaction.TargetAccount]; !alright {
					tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, final_balance - transaction.Amount, false})
				} else {
					for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID == transaction.ID {
							tentativeWriteTimestamps[transaction.TargetAccount][i].Balance = final_balance - transaction.Amount
							break
						}
					}
				}
				return "OK"
			} else {
				return "ABORTED"
			}
		} else {
			accounts[transaction.TargetAccount] = 0
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, transaction.Amount, false})
				return "OK"
			} else {
				return "ABORTED"
			}
		}
	} else {
		return "ABORTED"
	}
}

func handleBalance(transaction Transaction) string {
	var final_balance = 0
	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		//search across the committed timestamp and the TW list for object D, where D.ID <= transaction.ID
		for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
			if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID <= transaction.ID {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
					final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					if transaction.ID > readTimestamps[transaction.TargetAccount] {
						readTimestamps[transaction.TargetAccount] = transaction.ID
					}
				} else {
					if tentativeWriteTimestamps[transaction.TargetAccount][i].transactionID == transaction.ID {
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
					} else {
						// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
						return handleBalance(transaction)
					}
				}
			}
		}

		if final_balance != 0 {
			return transaction.TargetServer + "." + transaction.TargetAccount + " " + strconv.Itoa(final_balance)
		}

		// If no tentative write found, return the committed balance if it exists
		acct, ok := accounts[transaction.TargetAccount]
		if ok {
			if transaction.ID > readTimestamps[transaction.TargetAccount] {
				readTimestamps[transaction.TargetAccount] = transaction.ID
			}
			return transaction.TargetServer + "." + transaction.TargetAccount + " " + strconv.Itoa(acct)
		} else {
			// ACCOUNT NOT FOUND
			return "NOT FOUND, ABORTED"
		}
	} else {
		return "ABORTED"
	}
}
