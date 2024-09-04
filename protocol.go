package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func parseCommand(conn net.Conn) (*Command, error) {
	command := Command{}

	reader := bufio.NewReader(conn)

	parameters, err := decodeBulkArray(reader)
	if err != nil {
		return nil, err
	}

	command.Name = strings.ToUpper(parameters[0])
	command.Args = parameters[1:]

	if !isValidCommand(command.Name) {
		return nil, fmt.Errorf("unsupported command")
	}

	return &command, nil
}

func decodeBulkArray(reader *bufio.Reader) ([]string, error) {

	// read the first line to extract the total number of parameters
	firstLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	firstLine = strings.TrimSpace(firstLine)
	if firstLine[:1] != "*" {
		return nil, fmt.Errorf("parse error. expected *n\\r\\n")
	}
	parameterCount, err := strconv.Atoi(firstLine[1:])
	if err != nil {
		return nil, fmt.Errorf("error extracting number of parameters")
	}

	// extract all the paramters
	var parameters []string
	for i := 0; i < parameterCount; i++ {
		parameterLengthString, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("parse error. error reading request")
		}

		parameterLengthString = strings.TrimSpace(parameterLengthString)
		if parameterLengthString[:1] != "$" {
			fmt.Println("parse error")
		}
		_, err = strconv.Atoi(parameterLengthString[1:])
		if err != nil {
			return nil, fmt.Errorf("error extracting lengh of parameter")
		}

		parameter, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("parse error")
		}
		parameter = strings.TrimSpace(parameter)

		parameters = append(parameters, parameter)
	}
	return parameters, nil
}

func encodeRespString(data string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
}

func encodeBulkArray(data []string) []byte {
	var encoded string

	encoded = fmt.Sprintf("*%d\r\n", len(data))
	for _, v := range data {
		encoded += string(encodeRespString(v))
	}
	return []byte(encoded)
}

func encodeSimpleString(data string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", data))
}

func encodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}
