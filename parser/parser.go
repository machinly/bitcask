package parser

import (
	"fmt"
	"strings"

	"github.com/machinly/bitcask/engine"
)

type Parser interface {
	Parse(string) ([]string, error)
}

type parser struct {
	engine engine.Engine
}

func (p *parser) Parse(cmdStr string) ([]string, error) {
	if cmdStr == "" {
		return []string{""}, nil
	}
	rawCmds := strings.Split(cmdStr, " ")
	cmds := make([]string, 0, len(rawCmds))
	for _, rawCmd := range rawCmds {
		cmd := strings.TrimSpace(rawCmd)
		if cmd == "" {
			continue
		}
		cmds = append(cmds, cmd)
	}

	method := cmds[0]
	var args []string
	if len(cmds) > 1 {
		args = cmds[1:]
	}

	switch method {
	case "put":
		if len(args) != 2 {
			return nil, fmt.Errorf("put command requires 2 arguments")
		}
		err := p.engine.Put(args[0], args[1])
		if err != nil {
			return nil, err
		}
		return []string{"ok"}, nil
	case "get":
		if len(args) != 1 {
			return nil, fmt.Errorf("get command requires 1 argument")
		}
		value, err := p.engine.Get(args[0])
		if err != nil {
			return nil, err
		}
		return []string{value}, nil
	case "delete":
		if len(args) != 1 {
			return nil, fmt.Errorf("delete command requires 1 argument")
		}
		err := p.engine.Delete(args[0])
		if err != nil {
			return nil, err
		}
		return []string{"ok"}, nil
	case "list":
		if len(args) != 0 {
			return nil, fmt.Errorf("list command requires 0 arguments")
		}
		keys, err := p.engine.ListKeys()
		return keys, err
	default:
		return nil, fmt.Errorf("unknown command: %s", method)
	}
}

func NewParser(engine engine.Engine) Parser {
	return &parser{engine: engine}
}
