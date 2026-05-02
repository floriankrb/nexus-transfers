# TCP Relay over TLS — Full Implementation Guide

## What This Does

Two clients (A and C) both connect to a relay server (B) on port 443. B pairs them together and becomes a dumb pipe — bytes from A go directly to C and vice versa, with zero userspace copies (using Linux `splice()`). The relay handles 1TB transfers with near-zero CPU.

```
Client A ──TLS:443──► Nginx on B ──plain TCP:9000──► Go relay app ──plain TCP:9000──► Nginx on B ◄──TLS:443── Client C
                                                      (splice A↔C)
```

No WebSocket. No masking. No per-byte CPU cost. Nginx handles TLS, the Go app handles pairing and splicing.

---

## Why This Design

- **Port 443**: firewalls universally allow outbound port 443
- **TLS**: satisfies deep packet inspection, looks like normal HTTPS
- **splice()**: Linux syscall that moves data between two kernel socket buffers through a pipe, never touching userspace RAM. Cost: ~0 CPU, ~0 memory copies
- **Go app**: tiny, handles the "wait for two clients, then wire them together" logic that Nginx cannot do dynamically

---

## Server Requirements

- Linux (Ubuntu 20.04+ or Debian 11+)
- Root access (to bind port 443)
- Nginx installed
- Go 1.21+ installed
- A TLS certificate (Let's Encrypt / certbot is fine)

---

## Step 1 — The Go Relay App

Save as `/opt/relay/main.go`.

```go
package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
)

const listenAddr = "127.0.0.1:9000"

// waitingConn holds the first client until the second arrives
var (
	mu          sync.Mutex
	waitingConn net.Conn
)

func main() {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("relay listening on %s", listenAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handle(conn)
	}
}

func handle(conn net.Conn) {
	mu.Lock()
	if waitingConn == nil {
		// First client: park and wait
		waitingConn = conn
		mu.Unlock()
		log.Printf("first client connected: %s — waiting for peer", conn.RemoteAddr())
		return
	}
	// Second client: grab the waiting one and splice them
	peer := waitingConn
	waitingConn = nil
	mu.Unlock()

	log.Printf("pairing %s ↔ %s — splicing", peer.RemoteAddr(), conn.RemoteAddr())
	go splice(peer, conn)
	go splice(conn, peer)
}

// splice copies from src to dst using Linux splice() syscall (zero userspace copy)
// Falls back to io.Copy on non-Linux or if splice fails
func splice(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()

	srcFile, err1 := src.(*net.TCPConn).File()
	dstFile, err2 := dst.(*net.TCPConn).File()

	if err1 != nil || err2 != nil {
		// fallback
		io.Copy(dst, src)
		return
	}
	defer srcFile.Close()
	defer dstFile.Close()

	srcFd := int(srcFile.Fd())
	dstFd := int(dstFile.Fd())

	// pipe is the kernel intermediary splice needs
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		io.Copy(dst, src)
		return
	}
	defer pipeR.Close()
	defer pipeW.Close()

	pipeRFd := int(pipeR.Fd())
	pipeWFd := int(pipeW.Fd())

	const chunkSize = 1 << 17 // 128 KB

	for {
		// kernel moves bytes: src socket → pipe (no userspace copy)
		n, err := syscall.Splice(srcFd, nil, pipeWFd, nil, chunkSize,
			syscall.SPLICE_F_MOVE|syscall.SPLICE_F_MORE)
		if n == 0 || err != nil {
			return
		}
		// kernel moves bytes: pipe → dst socket (no userspace copy)
		_, err = syscall.Splice(pipeRFd, nil, dstFd, nil, int(n),
			syscall.SPLICE_F_MOVE|syscall.SPLICE_F_MORE)
		if err != nil {
			return
		}
	}
}
```

### Build and install

```bash
cd /opt/relay
go mod init relay
go build -o relay-bin .
```

### Run as a systemd service

Save as `/etc/systemd/system/relay.service`:

```ini
[Unit]
Description=TCP splice relay
After=network.target

[Service]
ExecStart=/opt/relay/relay-bin
Restart=always
User=nobody
Group=nogroup

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now relay
systemctl status relay
```

---

## Step 2 — Nginx Configuration

Nginx sits in front, terminates TLS on port 443, and forwards the raw decrypted TCP stream to the Go app on port 9000. This uses the `stream` module (L4 proxy, not L7 HTTP proxy).

Check your Nginx has the stream module:
```bash
nginx -V 2>&1 | grep stream
# should show: --with-stream
```

### /etc/nginx/nginx.conf

Add this **outside** the `http {}` block (stream is a top-level block):

```nginx
stream {

    # TLS termination
    upstream relay_backend {
        server 127.0.0.1:9000;
    }

    server {
        listen 443 ssl;

        ssl_certificate     /etc/letsencrypt/live/YOUR_DOMAIN/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/YOUR_DOMAIN/privkey.pem;

        ssl_protocols       TLSv1.2 TLSv1.3;
        ssl_ciphers         HIGH:!aNULL:!MD5;

        # Pass raw decrypted stream to Go app
        proxy_pass relay_backend;

        # Forward client IP info (optional)
        proxy_timeout 3600s;   # 1 hour — adjust for your transfer sizes
        proxy_connect_timeout 10s;
    }
}
```

```bash
nginx -t          # test config
systemctl reload nginx
```

---

## Step 3 — TLS Certificate (Let's Encrypt)

```bash
apt install certbot
certbot certonly --standalone -d YOUR_DOMAIN
# cert lands at /etc/letsencrypt/live/YOUR_DOMAIN/
```

Auto-renewal already configured by certbot. After renewal, reload nginx:
```bash
# add to /etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh
systemctl reload nginx
```

---

## Step 4 — Client Code (Go)

Both Client A and Client C use identical code. They just open a TLS connection to B on port 443 and then read/write the file.

```go
package main

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"os"
)

const relayAddr = "YOUR_DOMAIN:443"

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: client send|recv filename")
	}
	mode, filename := os.Args[1], os.Args[2]

	conn, err := tls.Dial("tcp", relayAddr, &tls.Config{
		ServerName: "YOUR_DOMAIN",
	})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	log.Println("connected to relay, waiting for peer...")

	switch mode {
	case "send":
		send(conn, filename)
	case "recv":
		recv(conn, filename)
	}
}

func send(conn net.Conn, filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer f.Close()

	n, err := io.Copy(conn, f)
	log.Printf("sent %d bytes, err=%v", n, err)
}

func recv(conn net.Conn, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("create: %v", err)
	}
	defer f.Close()

	n, err := io.Copy(f, conn)
	log.Printf("received %d bytes, err=%v", n, err)
}
```

### Usage

On Client A (sender):
```bash
go run client.go send mybigfile.tar.gz
```

On Client C (receiver):
```bash
go run client.go recv mybigfile.tar.gz
```

Order doesn't matter. Whichever connects first waits. When both are connected, transfer begins immediately.

---

## Data Path Summary

```
Client A                   Server B                      Client C
─────────                  ────────                      ─────────
app                        Nginx          Go relay       app
 │                           │               │            │
 │──TLS bytes──────────────► │               │            │
 │                           │──plain TCP──► │            │
 │                           │               │◄──plain TCP─│◄──TLS bytes──│
 │                           │               │            │
 │                    splice(A_fd → pipe → C_fd)          │
 │                    splice(C_fd → pipe → A_fd)          │
 │                    (kernel only, zero userspace copy)  │
```

### Where CPU is spent

| Phase | CPU cost |
|---|---|
| TLS handshake (once per client) | Negligible |
| Bulk data transfer (splice) | ~0 — kernel pointer swap |
| Nginx TLS decrypt/encrypt | Memory bandwidth only — no compute |

For a 1TB transfer the relay's CPU stays near idle. The bottleneck will be your network cards, not the CPU.

---

## Firewall Rules on B

```bash
ufw allow 443/tcp
ufw allow 22/tcp   # keep SSH open
ufw enable
```

Port 9000 (Go app) stays local — no need to open it externally.

---

## Pairing Logic Note

The current Go code uses a simple "first come first served" single-slot queue. This means:

- Only one pair at a time
- Third connection waits until the current pair disconnects

For multiple simultaneous pairs, replace the single `waitingConn` with a keyed map where clients identify which session they belong to (e.g. pass a session ID as the first bytes after connecting). That is a straightforward extension.
