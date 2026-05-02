package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

// wsGUID is the fixed WebSocket GUID defined in RFC 6455.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsAccept computes the Sec-WebSocket-Accept value from the client's key.
func wsAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// httpUpgrade reads an HTTP/1.1 upgrade request from br/conn and responds
// with 101 Switching Protocols.  After this call the connection is a raw
// byte pipe — no WebSocket framing or XOR masking is performed.
func httpUpgrade(conn net.Conn, br *bufio.Reader) error {
	// Read request line (e.g. "GET /relay HTTP/1.1")
	line, err := br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read request line: %w", err)
	}
	if !strings.HasPrefix(strings.TrimSpace(line), "GET ") {
		return fmt.Errorf("expected GET upgrade request, got: %q", strings.TrimSpace(line))
	}

	// Read headers until blank line, capture Sec-WebSocket-Key.
	var wsKey string
	for {
		line, err = br.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read headers: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-key:") {
			wsKey = strings.TrimSpace(line[len("sec-websocket-key:"):])
		}
	}
	if wsKey == "" {
		return fmt.Errorf("missing Sec-WebSocket-Key header")
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAccept(wsKey) + "\r\n\r\n"
	_, err = fmt.Fprint(conn, resp)
	return err
}

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

	// HTTP/1.1 upgrade: client sends a WebSocket-style GET, we respond 101.
	// After this the connection is raw bytes — no WS framing, no XOR.
	if err := httpUpgrade(conn, c.br); err != nil {
		log.Printf("upgrade failed from %s: %v", conn.RemoteAddr(), err)
		return
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
