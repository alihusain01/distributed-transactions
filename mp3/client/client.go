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

	fmt.Println("Type 'BEGIN' to initiate a connection.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "BEGIN" {
			fmt.Println("OK")
			connectToCoordinator()
			enterTransactions()
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
	randIndex := rand.Intn(len(servers))
	selectedServer := servers[randIndex]

	conn, err := net.Dial("tcp", selectedServer.Host+":"+selectedServer.Port)
	if err != nil {
		fmt.Println("Failed to connect to selected coordinator server.")
	}

	currentConnection = conn

	fmt.Printf("Connected to %s at %s on port %s\n", selectedServer.Name, selectedServer.Host, selectedServer.Port)

}

func enterTransactions() {
	txID := float64(time.Now().UnixNano()) / 1e9
  
	encoder := gob.NewEncoder(currentConnection)
	decoder := gob.NewDecoder(currentConnection)
  
	defer currentConnection.Close()
  
	scanner := bufio.NewScanner(os.Stdin)
	for {
	  if !scanner.Scan() {
		break
	  }
  
	  // Formatting user input into Transaction and sending it to coordinator
	  input := strings.Split(scanner.Text(), " ")
	  var command, branch, account string
	  var amount int
  
	  command = input[0]
  
	  if command == "COMMIT" {
		branch, account = "", ""
		amount = 0
	  }
  
	  if command == "BALANCE" {
		temp := strings.Split(input[1], ".")
		branch, account = temp[0], temp[1]
		amount = 0
	  }
  
	  // If the command is WITHDRAW or DEPOSIT, check for amount. Used to avoid index out of bounds.
	  if command == "WITHDRAW" || command == "DEPOSIT" {
		temp := strings.Split(input[1], ".")
		branch, account = temp[0], temp[1]
		amount, _ = strconv.Atoi(input[2])
	  }
  
	  var transaction = Transaction{MessageType: command, TargetServer: branch, TargetAccount: account, Amount: amount, ID: txID}
	  encoder.Encode(transaction)
  
	  // Await reply from server
	  var response string
	  err := decoder.Decode(&response)
  
	  if err != nil {
		fmt.Println("Failed to receive response from coordinator.", err)
		os.Exit(1)
	  }
  
	  if response == "ABORT" || response == "NOT FOUND, ABORTED" || response == "COMMIT OK" {
		fmt.Println(response)
		os.Exit(1)
	  }
  
	  fmt.Println(response)
	}
}
