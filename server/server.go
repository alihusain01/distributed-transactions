package main

import (
	"fmt"
	"os"
)

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

	fmt.Printf("Branch: %s\nConfig file: %s\n", branch, configFile)
}
