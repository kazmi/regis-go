package main

import (
	"fmt"
	"log"
)

const (
	CommandPing   = "PING"
	CommandEcho   = "ECHO"
	CommandSet    = "SET"
	CommandGet    = "GET"
	CommandConfig = "CONFIG"
	CommandKeys   = "KEYS"
)

type Command struct {
	Name string
	Args []string
}

func isValidCommand(name string) bool {
	switch name {
	case CommandPing, CommandEcho, CommandSet, CommandGet, CommandConfig, CommandKeys:
		return true
	default:
		return false
	}
}

func minArgumentsRequired(cmd *Command) int {
	switch cmd.Name {
	case CommandPing:
		return 0
	case CommandEcho, CommandGet, CommandKeys:
		return 1
	case CommandSet, CommandConfig:
		return 2
	default:
		return 0
	}
}

func checkArguments(cmd *Command) error {
	if len(cmd.Args) < minArgumentsRequired(cmd) {
		log.Printf("not enough arguments passed for `%s` command", cmd.Name)
		return fmt.Errorf("not enough arguments passed for `%s` command", cmd.Name)
	}
	return nil
}
