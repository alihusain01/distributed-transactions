package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

var connectedServers = make(map[string]bool)
var currentServer Server

type Server struct {
	Name string
	Host string
	Port string
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

// Function to establish connections to servers
func establishConnections(servers []Server) {
	for {
		for _, server := range servers {
			if connectedServers[server.Name] {
				continue // Skip already connected servers
			}
			conn, err := net.Dial("tcp", server.Host+":"+server.Port)
			if err != nil {
				fmt.Printf("Error connecting to %s at %s on port %s: %s\n", server.Name, server.Host, server.Port, err)
				time.Sleep(3 * time.Second) // Wait before retrying
				continue
			}
			connectedServers[server.Name] = true
			fmt.Printf("Connected to %s at %s on port %s\n", server.Name, server.Host, server.Port)
			defer conn.Close()
			// You can use 'conn' for further communication with the server if needed
		}

		allConnected := true
		for _, server := range servers {
			if !connectedServers[server.Name] {
				allConnected = false
				break
			}
		}
		if allConnected {
			break // Exit the loop if all servers are connected
		}
	}
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

	// Get the current server from the list
	for _, server := range servers {
		if server.Name == branch {
			currentServer = server
			break
		}
	}

	// Start listening on the specified port
	listener, err := net.Listen("tcp", ":"+currentServer.Port)
	if err != nil {
		fmt.Printf("Error starting server on port %s: %s\n", currentServer.Port, err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Printf("Server started on port %s\n", currentServer.Port)

	establishConnections(servers)

	fmt.Printf("Branch %s has been successfully connected to all servers.\n", branch)
}
