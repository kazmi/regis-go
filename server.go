package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"time"
)

type Redis struct {
	Addr   string
	Port   string
	Store  InMemoryKeyValueStore
	Config Configuration
}

func NewRedis(addr, port string, config Configuration) *Redis {

	redis := Redis{
		Addr: addr,
		Port: port,
		Store: InMemoryKeyValueStore{
			store:  make(map[string]string),
			expiry: make(map[string]time.Time),
		},
		Config: config,
	}

	if config.DbFileName != "" {
		rdb := RDB{}
		path := filepath.Join(config.Directory, config.DbFileName)
		err := rdb.Load(path)
		if err == nil {
			redis.RestoreFromFS(&rdb)
		}
	}

	return &redis
}

func (r *Redis) RestoreFromFS(rdb *RDB) error {

	for _, p := range rdb.Database {
		r.Store.Set(p.key, p.value)
	}

	r.Store.Dump()

	return nil
}

func (r *Redis) ListenAndServe() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", r.Addr, r.Port))
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		defer conn.Close()

		go func() {
			if err = r.handleConnection(conn); err != nil {
				slog.Error(fmt.Sprintf("redis error: %s", err))
			}
		}()
	}

}

func (r *Redis) handleConnection(conn net.Conn) error {
	// handle multiple commands
	for {
		redisCommand, err := parseCommand(conn)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err = r.executeCommand(conn, redisCommand); err != nil {
			return err
		}
	}

	return nil
}

func (r *Redis) executeCommand(w io.Writer, c *Command) error {

	checkArguments(c)

	switch c.Name {
	case CommandPing:
		_, err := w.Write(encodeSimpleString("PONG"))
		if err != nil {
			return err
		}

	case CommandEcho:
		_, err := w.Write(encodeSimpleString(strings.Join(c.Args, " ")))
		if err != nil {
			return err
		}

	case CommandSet:
		key := c.Args[0]
		value := c.Args[1]
		if len(c.Args) == 4 {
			expiresAt := c.Args[3]
			r.Store.SetWithExpiry(key, value, expiresAt)
		} else {
			r.Store.Set(key, value)
		}

		_, err := w.Write(encodeSimpleString("OK"))
		if err != nil {
			return err
		}

	case CommandGet:
		key := c.Args[0]
		value, ok := r.Store.Get(key)
		if ok {
			_, err := w.Write(encodeRespString(value))
			if err != nil {
				return err
			}
		} else {
			_, err := w.Write(encodeNullBulkString())
			if err != nil {
				return err
			}
		}

	case CommandConfig:
		subCommand := c.Args[0]

		if subCommand != "GET" {
			return fmt.Errorf("unsupported sub-command for 'config'")
		}

		if !isValidConfig(c.Args[1]) {
			return fmt.Errorf("unsupported config name")
		}

		switch c.Args[1] {
		case ConfigDir:
			_, err := w.Write(encodeBulkArray([]string{ConfigDir, r.Config.Directory}))
			if err != nil {
				return err
			}
		case ConfigDbFileName:
			_, err := w.Write(encodeBulkArray([]string{ConfigDbFileName, r.Config.DbFileName}))
			if err != nil {
				return err
			}
		}

	case CommandKeys:
		var keys []string
		for k := range r.Store.store {
			keys = append(keys, k)
		}
		if len(keys) > 0 {
			_, err := w.Write(encodeBulkArray(keys))
			if err != nil {
				return err
			}
		} else {
			_, err := w.Write(encodeNullBulkString())
			if err != nil {
				return err
			}
		}

	}
	return nil
}
