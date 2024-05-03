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
	TransactionID float64
	Balance       int
	IsCommitted   bool
	IsAborted     bool
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
		responseChan := make(chan string)

		err := clientDecoder.Decode(&transaction)
		if err != nil {
			fmt.Println("Failed to receive transaction from client: ", err)
			return
		}

		fmt.Println("Received transaction from Client", transaction)

		if transaction.MessageType == "COMMIT" {
			//TODO: Commit transaction
			fmt.Println("Received COMMIT command from client. Committing transaction.")

			go handleCommit(transaction, responseChan)
		} else if transaction.MessageType == "ABORT" {
			fmt.Println("Received ABORT command from client. Aborting transaction.")

			go handleAbort(transaction, responseChan)
			for server, _ := range connectedServers {
				go func(server string) {
					serverEncoder := encoders[server]
					serverEncoder.Encode(transaction)

				}(server)
			}
		} else if transaction.TargetServer == currentServer.Name {
			go transactionHandler(transaction, responseChan)
		} else {
			serverEncoder := encoders[transaction.TargetServer]
			serverEncoder.Encode(transaction)

			var dwb_response string;

			serverDecoder := decoders[transaction.TargetServer]
			serverDecoder.Decode(&dwb_response)
			fmt.Println("Received response from corresponding server: ", dwb_response)
		}

		response := <-responseChan

		if response == "ABORT" || response == "NOT FOUND, ABORTED" || response == "ABORTED" {
			fmt.Println("Response from server: ", response)
		}

		clientEncoder.Encode(response)
	}
}

func transactionFromCoordinator(conn net.Conn) {
	serverDecoder := gob.NewDecoder(conn)
	serverEncoder := gob.NewEncoder(conn)
	for {
		var transaction Transaction
		responseChan := make(chan string)
		err := serverDecoder.Decode(&transaction)

		if err != nil {
			fmt.Println("Client disconnected", err)
			return
		}

		fmt.Println("Received transaction from coordinator: ", transaction)
		go transactionHandler(transaction, responseChan)

		response := <-responseChan

		serverEncoder.Encode(response)
	}
}

func transactionHandler(transaction Transaction, responseChan chan<- string){
	if transaction.MessageType == "DEPOSIT" {
		go handleDeposit(transaction, responseChan)
	} else if transaction.MessageType == "WITHDRAW" {
		go handleWithdrawal(transaction, responseChan)
	} else if transaction.MessageType == "BALANCE" {
		go handleBalance(transaction, responseChan)
	} else if transaction.MessageType == "PREPARE" {
		go handlePrepare(transaction, responseChan)
	} else if transaction.MessageType == "COMMIT" {
		go finalCommit(transaction, responseChan)
	} else {
		go handleAbort(transaction, responseChan)
	}
}

func handleDeposit(transaction Transaction, responseChan chan<- string) {
	var final_balance int
	fmt.Println("Latest write timestamp: ", latestWriteTimeStamp[transaction.TargetAccount])

	fmt.Println("First Deposit Check: ", accounts[transaction.TargetAccount])

	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		fmt.Println("bp1")
		for {
			target_tent_write := TentativeWrites{}
			for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID <= transaction.ID {
					fmt.Println("bp2")
					if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
						fmt.Println("bp3")
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						if transaction.ID > readTimestamps[transaction.TargetAccount] {
							readTimestamps[transaction.TargetAccount] = transaction.ID
							fmt.Println("bp4")
						}
					} else {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID == transaction.ID {
							fmt.Println("Tentative Write Stamps for Account", transaction.TargetAccount)
							for _, tw := range tentativeWriteTimestamps[transaction.TargetAccount] {
								fmt.Println("Transaction ID:", tw.TransactionID)
								fmt.Println("Balance:", tw.Balance)
							}
							final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						} else {
							// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
							fmt.Println("Waiting for transaction to commit or abort inside of handleDeposit")

							target_tent_write = tentativeWriteTimestamps[transaction.TargetAccount][i]
							continue
							// for {
							// 	if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted || tentativeWriteTimestamps[transaction.TargetAccount][i].IsAborted {
							// 		break
							// 	}
							// }
							// return handleDeposit(transaction)
						}
					}
				}
			}
			if target_tent_write.IsCommitted || target_tent_write.IsAborted || len(tentativeWriteTimestamps[transaction.TargetAccount]) == 0{
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// If no tentative write found, final balance is the committed balance
		if len(tentativeWriteTimestamps[transaction.TargetAccount]) == 0 {
			println("bp10")
			final_balance = accounts[transaction.TargetAccount]
		}

		fmt.Println("Second Deposit Check: ", accounts[transaction.TargetAccount])

		if _, ok := accounts[transaction.TargetAccount]; ok {
			println("bp11")
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				//Perform a tentative write on D: If Tc already has an entry in the TW list for D, update it. Else, add Tc and its write value to the TW list.
				println("bp12")
				print("PRINTING AFTER BP12")
				fmt.Println(tentativeWriteTimestamps[transaction.TargetAccount])
				if len(tentativeWriteTimestamps[transaction.TargetAccount]) == 0 {
					println("bp13")
					tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, final_balance + transaction.Amount, false, false})
				} else {
					println("bp14")
					for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID == transaction.ID {
							fmt.Println("Third Deposit Check: ", accounts[transaction.TargetAccount])
							tentativeWriteTimestamps[transaction.TargetAccount][i].Balance = final_balance + transaction.Amount
							break
						}
					}
				}
				responseChan <- "OK"
				return
			} else {
				responseChan <- "ABORTED"
				return
			}
		} else {
			accounts[transaction.TargetAccount] = 0
			println("bp15")
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				println("bp16")
				tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, transaction.Amount, false, false})
				responseChan <- "OK"
				return
			} else {
				responseChan <- "ABORTED"
				return
			}
		}
	} else {
		responseChan <- "ABORTED"
		return
	}
}

func handleWithdrawal(transaction Transaction, responseChan chan<- string) {
	if _, ok := accounts[transaction.TargetAccount]; !ok {
		responseChan <- "NOT FOUND, ABORTED"
		return
	}

	var final_balance int
	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		for {
			target_tent_write := TentativeWrites{}
			for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID <= transaction.ID {
					fmt.Println("W: bp2")
					if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
						fmt.Println("W: bp3")
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						if transaction.ID > readTimestamps[transaction.TargetAccount] {
							readTimestamps[transaction.TargetAccount] = transaction.ID
							fmt.Println("W: bp4")
						}
					} else {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID == transaction.ID {
							fmt.Println("Tentative Write Stamps for Account", transaction.TargetAccount)
							for _, tw := range tentativeWriteTimestamps[transaction.TargetAccount] {
								fmt.Println("Transaction ID:", tw.TransactionID)
								fmt.Println("Balance:", tw.Balance)
							}
							final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						} else {
							// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
							fmt.Println("Waiting for transaction to commit or abort inside of handleWithdraw")

							target_tent_write = tentativeWriteTimestamps[transaction.TargetAccount][i]
							continue
							// for {
							// 	if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted || tentativeWriteTimestamps[transaction.TargetAccount][i].IsAborted {
							// 		break
							// 	}
							// }
							// return handleDeposit(transaction)
						}
					}
				}
			}
			if target_tent_write.IsCommitted || target_tent_write.IsAborted || len(tentativeWriteTimestamps[transaction.TargetAccount]) == 0{
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if _, ok := accounts[transaction.TargetAccount]; ok {
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				//Perform a tentative write on D: If Tc already has an entry in the TW list for D, update it. Else, add Tc and its write value to the TW list.
				if _, alright := tentativeWriteTimestamps[transaction.TargetAccount]; !alright {
					tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, final_balance - transaction.Amount, false, false})
				} else {
					for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID == transaction.ID {
							tentativeWriteTimestamps[transaction.TargetAccount][i].Balance = final_balance - transaction.Amount
							break
						}
					}
				}
				responseChan <- "OK"
				return
			} else {
				responseChan <- "ABORTED"
				return
			}
		} else {
			accounts[transaction.TargetAccount] = 0
			if transaction.ID >= readTimestamps[transaction.TargetAccount] && transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
				tentativeWriteTimestamps[transaction.TargetAccount] = append(tentativeWriteTimestamps[transaction.TargetAccount], TentativeWrites{transaction.ID, transaction.Amount, false, false})
				responseChan <- "OK"
				return
			} else {
				responseChan <- "ABORTED"
				return
			}
		}
	} else {
		responseChan <- "ABORTED"
		return
	}
}

func handleBalance(transaction Transaction, responseChan chan<- string) {
	if _, ok := accounts[transaction.TargetAccount]; !ok {
		responseChan <- "NOT FOUND, ABORTED"
		return
	}

	var final_balance = -1
	if transaction.ID > latestWriteTimeStamp[transaction.TargetAccount] {
		for {
			target_tent_write := TentativeWrites{}
			for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
				if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID <= transaction.ID {
					fmt.Println("bp2")
					if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted {
						fmt.Println("bp3")
						final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						if transaction.ID > readTimestamps[transaction.TargetAccount] {
							readTimestamps[transaction.TargetAccount] = transaction.ID
							fmt.Println("bp4")
						}
					} else {
						if tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID == transaction.ID {
							fmt.Println("Tentative Write Stamps for Account", transaction.TargetAccount)
							for _, tw := range tentativeWriteTimestamps[transaction.TargetAccount] {
								fmt.Println("Transaction ID:", tw.TransactionID)
								fmt.Println("Balance:", tw.Balance)
							}
							final_balance = tentativeWriteTimestamps[transaction.TargetAccount][i].Balance
						} else {
							// Reapply read rule TODO: Add wait until the transaction that wrote Ds is committed or aborted
							fmt.Println("Waiting for transaction to commit or abort inside of handleDeposit")

							target_tent_write = tentativeWriteTimestamps[transaction.TargetAccount][i]
							continue
							// for {
							// 	if tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted || tentativeWriteTimestamps[transaction.TargetAccount][i].IsAborted {
							// 		break
							// 	}
							// }
							// return handleDeposit(transaction)
						}
					}
				}
			}
			if target_tent_write.IsCommitted || target_tent_write.IsAborted || len(tentativeWriteTimestamps[transaction.TargetAccount]) == 0{
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if final_balance != -1 {
			responseChan <- transaction.TargetServer + "." + transaction.TargetAccount + " " + strconv.Itoa(final_balance)
			return
		}

		// If no tentative write found, return the committed balance if it exists
		acct, ok := accounts[transaction.TargetAccount]
		if ok {
			if transaction.ID > readTimestamps[transaction.TargetAccount] {
				readTimestamps[transaction.TargetAccount] = transaction.ID
			}
			responseChan <- transaction.TargetServer + "." + transaction.TargetAccount + " " + strconv.Itoa(acct)
			return
		} else {
			// ACCOUNT NOT FOUND
			responseChan <- "NOT FOUND, ABORTED"
			return
		}
	} else {
		responseChan <- "ABORTED"
		return
	}
}

func handleCommit(transaction Transaction, responseChan chan <- string) {
	fmt.Println("Received Commit command from Client. Beginning 2 Phase Commit.")
	// First Phase
	transaction.MessageType = "PREPARE"

	responsesReceived := 0
	receivedAbort := false

	for server, _ := range connectedServers {
		go func(server string) {
			var response string
			serverEncoder := encoders[server]
			serverEncoder.Encode(transaction)

			serverDecoder := decoders[server]
			serverDecoder.Decode(&response)

			fmt.Println("Received prepare status response from ", server, response)

			if response == "ABORTED" {
				receivedAbort = true
			} else {
				responsesReceived += 1
			}
		}(server)
	}

	for {
		if responsesReceived == len(connectedServers) {
			break
		} else if receivedAbort {
			transaction.MessageType = "ABORT"
			handleAbort(transaction, responseChan)
			for server, _ := range connectedServers {
				go func(server string) {
					serverEncoder := encoders[server]
					serverEncoder.Encode(transaction)

				}(server)
			}

			responseChan <- "ABORTED"
			return

		}
	}

	// Second Phase
	transaction.MessageType = "COMMIT"

	finalCommit(transaction, responseChan)

	for server, _ := range connectedServers {
		go func(server string) {
			var response string
			serverEncoder := encoders[server]
			serverEncoder.Encode(transaction)

			serverDecoder := decoders[server]
			serverDecoder.Decode(&response)

			fmt.Println("Received commit status response from ", server, response)
		}(server)
	}

	for key, value := range accounts {
		fmt.Println(key, value)
	}

	responseChan <- "COMMIT OK"
	return
}

func handlePrepare(transaction Transaction, responseChan chan<- string){
	// FIRST WAIT FOR ALL PREVIOUS TENTATIVE WRITES TO BE COMMITTED OR ABORTED
	for i := 0; i < len(tentativeWriteTimestamps[transaction.TargetAccount]); i++ {
		if !tentativeWriteTimestamps[transaction.TargetAccount][i].IsCommitted && tentativeWriteTimestamps[transaction.TargetAccount][i].TransactionID != transaction.ID {
			fmt.Println("FAILED TO COMMIT. WAITING ON PREVIOUS TRANSACTIONS TO COMMIT.")
			// TODO: Add wait until the transaction that wrote Ds is committed or aborted
		} else {
			if tentativeWriteTimestamps[transaction.TargetAccount][i].Balance < 0 {
				fmt.Println("TRANSACTION RESULTS ON NEGATIVE BALANCE. CANNOT COMMIT.")
				responseChan <- "ABORTED"
				return
			}
		}

	}

	fmt.Println("Received prepare message from coordinator: ", transaction)

	// READY TO COMMIT
	responseChan <- "OK"
	return
}

func finalCommit(transaction Transaction, responseChan chan<- string) {
	for account, _ := range accounts {
		fmt.Println("Current TWL for Account ", account, ":", tentativeWriteTimestamps[account])

		for i := 0; i < len(tentativeWriteTimestamps[account]); i++ {
			if tentativeWriteTimestamps[account][i].TransactionID == transaction.ID && !tentativeWriteTimestamps[account][i].IsCommitted {
				fmt.Println("Entry ", tentativeWriteTimestamps[account][i])
				accounts[account] += tentativeWriteTimestamps[account][i].Balance
				tentativeWriteTimestamps[account][i].IsCommitted = true
				latestWriteTimeStamp[account] = transaction.ID
				// Finished committing, need to add wait being done here

				// Removing the tentative write timestamp from this list
				tentativeWriteTimestamps[account] = append(tentativeWriteTimestamps[account][:i], tentativeWriteTimestamps[account][i+1:]...)
			}
		}
	}
	responseChan <- "COMMIT OK"
	return
}

func handleAbort(transaction Transaction, responseChan chan<- string) {
	for account, _ := range accounts {
		for i := 0; i < len(tentativeWriteTimestamps[account]); i++ {
			if tentativeWriteTimestamps[account][i].TransactionID == transaction.ID {
				if transaction.MessageType == "WITHDRAW" {
					tentativeWriteTimestamps[account][i].Balance += transaction.Amount
				} else if transaction.MessageType == "DEPOSIT" {
					tentativeWriteTimestamps[account][i].Balance -= transaction.Amount
				}
				tentativeWriteTimestamps[account][i].IsAborted = true
			}
		}
	}
	responseChan <- "ABORTED"
	return
}
