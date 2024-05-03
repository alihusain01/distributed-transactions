package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
	"encoding/gob"
	"strconv"
)

var servers []Server
var currentCoordinator string
var currentConnection net.Conn

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

	configFile := args[1]

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Println("The specified config file does not exist.")
		os.Exit(1)
	}

	err := readConfigFile(configFile)
	if err != nil {
		fmt.Println("Error reading config file:", err)
		os.Exit(1)
	}

	// fmt.Println("Type 'BEGIN' to initiate a connection.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "BEGIN" {
			fmt.Println("OK")
			connectToCoordinator()
			startTransactions()
		}
	}
}

func readConfigFile(filename string) (error) {
	file, err := os.Open(filename)
	if err != nil {
		return err
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
		return err
	}
	return nil
}

func connectToCoordinator() {
	// Select a random server to connect to
	for{
		randIndex := rand.Intn(len(servers))
		serverInfo := servers[randIndex]
	
		conn, err := net.Dial("tcp", serverInfo.Host+":"+serverInfo.Port)
		if err != nil {
			// fmt.Println("Failed to connect to selected coordinator server: ", randIndex)
		}else{
			// fmt.Println("Connected to coordinator server: ", randIndex)
			currentConnection = conn
			break
		}
	}
}

func startTransactions() {
	currentDecoder := gob.NewDecoder(currentConnection)
	currentEncoder := gob.NewEncoder(currentConnection)

	transactionID := float64(time.Now().UnixNano()) / 1e9
  
	defer currentConnection.Close()
  
	scanner := bufio.NewScanner(os.Stdin)
	for {
	  if !scanner.Scan() {
		break
	  }

	  input := strings.Split(scanner.Text(), " ")
	  var messageType, targetServer, targetAccount string
	  var amount int
  
	  messageType = input[0]
  
	  if messageType == "COMMIT" || messageType == "ABORT" {
		targetServer, targetAccount = "", ""
		amount = 0
	  }
  
	  if messageType == "BALANCE" {
		temp := strings.Split(input[1], ".")
		targetServer, targetAccount = temp[0], temp[1]
		amount = 0
	  }
  
	  if messageType == "DEPOSIT" || messageType == "WITHDRAW" {
		temp := strings.Split(input[1], ".")
		targetServer, targetAccount = temp[0], temp[1]
		amount, _ = strconv.Atoi(input[2])
	  }
  
	  var transaction = Transaction{MessageType: messageType, TargetServer: targetServer, TargetAccount: targetAccount, Amount: amount, ID: transactionID}
	  currentEncoder.Encode(transaction)
  
	  // Await reply from server
	  var response string
	  err := currentDecoder.Decode(&response)
  
	  if err != nil {
		fmt.Println("Failed to receive response from coordinator.", err)
		os.Exit(0)
	  }
  
	  if response == "ABORTED" || response == "NOT FOUND, ABORTED" || response == "COMMIT OK" {
		fmt.Println(response)
		os.Exit(0)
	  }
  
	  fmt.Println(response)
	}
}
