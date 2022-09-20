package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/machinly/bitcask/engine"
	"github.com/machinly/bitcask/parser"
)

var (
	flagDirName = flag.String("dir", "", "directory name")
)

func main() {
	flag.Parse()
	if *flagDirName == "" {
		_flagDirName := "./dbdata"
		flagDirName = &_flagDirName
	}
	bitcask, err := engine.OpenBitcaskEngine(*flagDirName)
	if err != nil {
		panic(err)
	}
	defer bitcask.Close()

	p := parser.NewParser(bitcask)
	err = repl(p)
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}

func repl(p parser.Parser) error {
	stdin := bufio.NewReader(os.Stdin)
	for {
		// read
		fmt.Print(">>> ")
		line, prefix, err := stdin.ReadLine()
		if prefix {
			return fmt.Errorf("line too long")
		}
		if err != nil {
			return err
		}
		cmd := strings.TrimSpace(string(line))
		if cmd == "exit" {
			break
		}

		// eval
		values, err := p.Parse(cmd)

		// print
		if err != nil {
			fmt.Printf("E %v\n", err)
			continue
		}

		for _, v := range values {
			fmt.Println(v)
		}
	}
	return nil
}
