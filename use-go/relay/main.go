package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const listenAddr = "127.0.0.1:9000"

type client struct {
	name string
	conn net.Conn
	br   *bufio.Reader
	done chan struct{}
}

func (c *client) writeLine(s string) error {
	_, err := fmt.Fprintf(c.conn, "%s\n", s)
	return err
}

func (c *client) readLine() (string, error) {
	line, err := c.br.ReadString('\n')
	return strings.TrimSpace(line), err
}

func (c *client) reader() io.Reader {
	return io.MultiReader(c.br, c.conn)
}

var (
	mu       sync.Mutex
	registry = map[string][]*client{}
)

func enqueue(c *client) {
	mu.Lock()
	registry[c.name] = append(registry[c.name], c)
	mu.Unlock()
	log.Printf("+ %s (%s) queue=%d", c.name, c.conn.RemoteAddr(), len(registry[c.name]))
}

func dequeue(c *client) {
	mu.Lock()
	defer mu.Unlock()
	q := registry[c.name]
	for i, x := range q {
		if x == c {
			registry[c.name] = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(registry[c.name]) == 0 {
		delete(registry, c.name)
	}
	log.Printf("- %s", c.name)
}

func pop(name string) *client {
	mu.Lock()
	defer mu.Unlock()
	q := registry[name]
	if len(q) == 0 {
		return nil
	}
	c := q[0]
	registry[name] = q[1:]
	if len(registry[name]) == 0 {
		delete(registry, name)
	}
	return c
}

func main() {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("relay listening on %s", listenAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
	c := &client{
		conn: conn,
		br:   bufio.NewReader(conn),
		done: make(chan struct{}),
	}

	line, err := c.readLine()
	if err != nil {
		return
	}
	verb, name, ok := strings.Cut(line, " ")
	if !ok || verb != "HELLO" {
		c.writeLine("ERR expected: HELLO <name>")
		return
	}
	name = strings.TrimSpace(name)
	if name == "" || name == "me" {
		c.writeLine("ERR invalid name")
		return
	}
	c.name = name
	c.writeLine("OK")

	line, err = c.readLine()
	if err != nil {
		return
	}
	switch {
	case line == "SERVE":
		enqueue(c)
		defer dequeue(c)
		<-c.done

	case strings.HasPrefix(line, "COPY "):
		handleCopy(c, strings.TrimPrefix(line, "COPY "))

	default:
		c.writeLine("ERR unknown command; expected SERVE or COPY <src> <dst>")
	}
}

func parseSpec(spec string) (name, path string) {
	n, p, ok := strings.Cut(spec, ":")
	if !ok {
		return "", spec
	}
	if n == "me" {
		n = ""
	}
	return n, p
}

func handleCopy(initiator *client, args string) {
	fields := strings.Fields(args)
	if len(fields) != 2 {
		initiator.writeLine("ERR usage: COPY <src> <dst>")
		return
	}
	srcName, _ := parseSpec(fields[0])
	dstName, _ := parseSpec(fields[1])

	switch {
	case srcName == "" && dstName == "":
		initiator.writeLine("ERR one side must name a remote client")
		return
	case srcName != "" && dstName != "":
		initiator.writeLine("ERR both sides are remote; one must be local")
		return
	}

	var peerName, peerCmd string
	if srcName != "" {
		_, srcPath := parseSpec(fields[0])
		peerName, peerCmd = srcName, "SEND "+srcPath
	} else {
		_, dstPath := parseSpec(fields[1])
		peerName, peerCmd = dstName, "RECV "+dstPath
	}

	peer := pop(peerName)
	if peer == nil {
		initiator.writeLine(fmt.Sprintf("ERR no available connection for %q", peerName))
		return
	}

	if err := peer.writeLine(peerCmd); err != nil {
		initiator.writeLine(fmt.Sprintf("ERR peer %s unreachable: %v", peerName, err))
		close(peer.done)
		return
	}
	initiator.writeLine("GO")

	log.Printf("bridge %s <-> %s", initiator.name, peer.name)

	// Bidirectional bridge: the application protocol needs both directions
	// (receiver sends RESUME back to sender at the start of each transfer).
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(peer.conn, initiator.reader())
		halfClose(peer.conn)
	}()
	go func() {
		defer wg.Done()
		io.Copy(initiator.conn, peer.reader())
		halfClose(initiator.conn)
	}()
	wg.Wait()

	log.Printf("done %s <-> %s", initiator.name, peer.name)
	close(peer.done)
}

func halfClose(c net.Conn) {
	type halfCloser interface{ CloseWrite() error }
	if hc, ok := c.(halfCloser); ok {
		hc.CloseWrite()
	}
}
