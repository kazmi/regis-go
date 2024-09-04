package main

const (
	ConfigDir        = "dir"
	ConfigDbFileName = "dbfilename"
)

type Configuration struct {
	Directory  string
	DbFileName string
}

func isValidConfig(name string) bool {
	switch name {
	case ConfigDir, ConfigDbFileName:
		return true
	default:
		return false
	}
}
