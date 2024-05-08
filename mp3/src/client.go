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

    if len(args) < 2 || len(args) > 3 {
        fmt.Println("Usage: [executable] [branch A-E] [config file] [optional: input file]")
        os.Exit(1)
    }

    configFile := args[1]
    inputFile := "input.txt" // Default input file name
    if len(args) == 3 {
        inputFile = args[2]
    }

    file, err := os.Open(configFile)
    if err != nil {
        fmt.Println("Error opening config file:", err)
        os.Exit(1)
    }
    defer file.Close()

    err = readConfigFile(file)
    if err != nil {
        fmt.Println("Error reading config file:", err)
        os.Exit(1)
    }

    var scanner *bufio.Scanner
    if _, err := os.Stat(inputFile); err == nil {
        fmt.Println("Using file:", inputFile)
        file, err := os.Open(inputFile)
        if err != nil {
            fmt.Println("Failed to open input file:", err)
            os.Exit(1)
        }
        defer file.Close()
        scanner = bufio.NewScanner(file)
    } else {
        scanner = bufio.NewScanner(os.Stdin)
    }

    for scanner.Scan() {
        input := scanner.Text()
        if input == "BEGIN" {
            fmt.Println("OK")
            connectToCoordinator()
            startTransactions(scanner)
        }
    }
}

func readConfigFile(file *os.File) error {
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
    return scanner.Err()
}

func connectToCoordinator() {
    // Select a random server to connect to
    for {
        randIndex := rand.Intn(len(servers))
        serverInfo := servers[randIndex]
    
        conn, err := net.Dial("tcp", serverInfo.Host+":"+serverInfo.Port)
        if err != nil {
            // fmt.Println("Failed to connect to selected coordinator server:", err)
        } else {
            // fmt.Println("Connected to coordinator server:", serverInfo.Name)
            currentConnection = conn
            break
        }
    }
}

func startTransactions(scanner *bufio.Scanner) {
    currentDecoder := gob.NewDecoder(currentConnection)
    currentEncoder := gob.NewEncoder(currentConnection)

    transactionID := float64(time.Now().UnixNano()) / 1e9
    defer currentConnection.Close()
  
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
    
        var transaction = Transaction{
            MessageType: messageType, 
            TargetServer: targetServer, 
            TargetAccount: targetAccount, 
            Amount: amount, 
            ID: transactionID,
        }
        currentEncoder.Encode(transaction)
    
        // Await reply from server
        var response string
        err := currentDecoder.Decode(&response)
    
        if err != nil {
            // fmt.Println("Failed to receive response from coordinator.", err)
            os.Exit(0)
        }
    
        if response == "ABORTED" || response == "NOT FOUND, ABORTED" || response == "COMMIT OK" {
            fmt.Println(response)
            os.Exit(0)
        }
    
        fmt.Println(response)
    }
}
