package main

import (
    "bufio"
    "fmt"
    "net"
    "os"
    "strings"
    "time"
)

var connectedServers = make(map[string]Server)
var currentServer Server
var numServers int

type Server struct {
    Name string
    Host string
    Port string
    Conn net.Conn
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
            conn, err := net.Dial("tcp", server.Host+":"+server.Port)
            if err != nil {
                fmt.Printf("Error connecting to %s at %s on port %s: %s\n", server.Name, server.Host, server.Port, err)
                time.Sleep(1 * time.Second)
                continue
            }
            connectedServers[server.Name] = Server{Name: server.Name, Host: server.Host, Port: server.Port, Conn: conn}
            fmt.Printf("Connected to %s at %s on port %s\n", server.Name, server.Host, server.Port)
        }
    }
}

func handleIncomingConnections(listener net.Listener, servers []Server) {
    for {
		if len(connectedServers) >= numServers {
            return
        }
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Failed to accept connection:", err)
            continue
        }
        go func(c net.Conn) {
            remoteAddr := c.RemoteAddr().String()
            parts := strings.Split(remoteAddr, ":")
            host := parts[0]
            // Search through servers to find the matching server by IP
            found := false
            for _, server := range servers {
                if server.Host == host {
                    server.Conn = c // Update the connection in the server struct
                    connectedServers[server.Name] = server // Store by server name instead of host IP
                    fmt.Printf("Added new connected server: %s at %s\n", server.Name, server.Host)
                    found = true
                    break
                }
            }
            if !found {
                fmt.Printf("Received connection from unknown host: %s\n", host)
            }
            if len(connectedServers) >= numServers {
                fmt.Println("All expected connections have been established.")
                return
            }
        }(conn)

        if len(connectedServers) >= numServers {
            return
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
	numServers = len(servers)

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
    defer listener.Close()
    fmt.Printf("Server started on port %s\n", currentServer.Port)

    go establishConnections(servers)
    handleIncomingConnections(listener, servers)

    fmt.Printf("Branch %s has been successfully connected to all servers.\n", branch)
}
