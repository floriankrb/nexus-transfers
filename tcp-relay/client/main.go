// Package transfers is a relay-based file-transfer client.
//
// Each call opens a fresh TLS connection to the relay, so multiple goroutines
// can call Copy / Serve concurrently without sharing state.
//
// Spec format: "peerName:/remote/path"  |  "./local/path"  |  "me:/local/path"
// Exactly one of src/dst must name a remote peer; the other must be local.
//
// After the relay handshake (GO / SEND / RECV) the two client processes speak
// a binary application protocol directly through the relay pipe:
//
//   MANIFEST → RESUME → (FILE_START → CHUNK… → FILE_DONE)… → DONE → DONE-ack
//
// Resume state is persisted in <dst>/.transfers/*.state so that a crash or
// network interruption can be continued exactly from the last chunk boundary.
package transfers

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

// ── public API ───────────────────────────────────────────────────────────────

// Copy transfers a file or directory tree between this client (identified as
// name) and a remote peer.  Exactly one of src/dst must be "peer:/path"; the
// other is a local path.
//
// Directories are transferred recursively.  Interrupted transfers resume
// automatically from the last 4 MiB chunk boundary.
func Copy(relay, name, src, dst string, opts ...Option) error {
	o := applyOpts(opts)
	s, err := dial(relay, name, o)
	if err != nil {
		return err
	}
	defer s.close()

	s.sendLine(fmt.Sprintf("COPY %s %s", src, dst))
	if resp := s.recvLine(); resp != "GO" {
		return fmt.Errorf("relay: %s", resp)
	}

	rw := io.MultiReader(s.br, s.conn) // drain any buffered bytes first

	srcName, srcPath := parseSpec(src)
	_, dstPath := parseSpec(dst)

	if srcName != "" {
		// remote → local: we receive
		return RunReceiver(rw, s.conn, dstPath, o.progress)
	}
	// local → remote: we send
	return RunSender(rw, s.conn, srcPath, o.progress)
}

// Serve registers this client as name and handles one incoming transfer.
// Blocks until the transfer completes.
func Serve(relay, name string, opts ...Option) error {
	o := applyOpts(opts)
	s, err := dial(relay, name, o)
	if err != nil {
		return err
	}
	defer s.close()

	s.sendLine("SERVE")

	cmd := s.recvLine()
	verb, path, _ := strings.Cut(cmd, " ")
	path = strings.TrimSpace(path)

	rw := io.MultiReader(s.br, s.conn)

	switch verb {
	case "SEND":
		return RunSender(rw, s.conn, path, o.progress)
	case "RECV":
		return RunReceiver(rw, s.conn, path, o.progress)
	default:
		return fmt.Errorf("unexpected relay command: %s", cmd)
	}
}

// CopyMany runs len(pairs) copies concurrently (one goroutine per pair).
// Each pair is [2]string{src, dst}.  The first error is returned after all
// goroutines have finished.
func CopyMany(relay, name string, pairs [][2]string, opts ...Option) error {
	errs := make([]error, len(pairs))
	var wg sync.WaitGroup
	for i, p := range pairs {
		wg.Add(1)
		go func(i int, src, dst string) {
			defer wg.Done()
			errs[i] = Copy(relay, name, src, dst, opts...)
		}(i, p[0], p[1])
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ServeLoop starts n concurrent Serve workers under the same name.
// Blocks until all n transfers complete.
func ServeLoop(relay, name string, n int, opts ...Option) error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = Serve(relay, name, opts...)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ── options ───────────────────────────────────────────────────────────────────

type options struct {
	insecure bool
	port     int
	progress *Progress
}

// Option configures a transfer call.
type Option func(*options)

// Insecure skips TLS certificate verification. For internal/development use only.
func Insecure() Option { return func(o *options) { o.insecure = true } }

// WithPort sets the relay port (default 443).
func WithPort(p int) Option { return func(o *options) { o.port = p } }

// WithProgress attaches a Progress reporter. Create one with NewProgress(os.Stderr).
// The caller must call prog.Stop() after the transfer is done.
func WithProgress(prog *Progress) Option { return func(o *options) { o.progress = prog } }

func applyOpts(opts []Option) options {
	o := options{port: 443}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// ── relay session ─────────────────────────────────────────────────────────────

type session struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func (s *session) sendLine(line string) {
	fmt.Fprintf(s.bw, "%s\n", line)
	s.bw.Flush()
}

func (s *session) recvLine() string {
	line, _ := s.br.ReadString('\n')
	return strings.TrimSpace(line)
}

func (s *session) close() { s.conn.Close() }

func dial(relay, name string, o options) (*session, error) {
	addr := fmt.Sprintf("%s:%d", relay, o.port)
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		ServerName:         relay,
		InsecureSkipVerify: o.insecure, //nolint:gosec
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	s := &session{
		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
	if err := s.httpUpgrade(relay); err != nil {
		conn.Close()
		return nil, fmt.Errorf("HTTP upgrade: %w", err)
	}
	s.sendLine("HELLO " + name)
	if resp := s.recvLine(); resp != "OK" {
		conn.Close()
		return nil, fmt.Errorf("handshake rejected: %s", resp)
	}
	return s, nil
}

// httpUpgrade sends an HTTP/1.1 WebSocket-style upgrade request and validates
// the 101 response.  After this the connection is a raw byte pipe.
func (s *session) httpUpgrade(host string) error {
	// Generate a random 16-byte key, base64-encoded (RFC 6455 §4.1).
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	key := base64.StdEncoding.EncodeToString(raw)

	req := "GET /relay HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err := fmt.Fprint(s.conn, req); err != nil {
		return err
	}

	// Read status line.
	status, err := s.br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.TrimSpace(status), "HTTP/1.1 101") {
		return fmt.Errorf("expected 101, got: %s", strings.TrimSpace(status))
	}

	// Validate Sec-WebSocket-Accept, then drain remaining headers.
	wantAccept := wsAccept(key)
	var gotAccept string
	for {
		line, err := s.br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") {
			gotAccept = strings.TrimSpace(line[len("sec-websocket-accept:"):])
		}
	}
	if gotAccept != wantAccept {
		return fmt.Errorf("Sec-WebSocket-Accept mismatch (got %q want %q)", gotAccept, wantAccept)
	}
	return nil
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
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
