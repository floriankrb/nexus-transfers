package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"transfers"
)

func main() {
	relay      := flag.String("relay", "", "relay hostname, e.g. relay.example.com (required)")
	name       := flag.String("name", "", "well-known name for this machine, e.g. machineA (required)")
	port       := flag.Int("port", 443, "relay port")
	insecure   := flag.Bool("insecure", false, "skip TLS certificate verification")
	noProgress := flag.Bool("no-progress", false, "disable the terminal progress bar")
	flag.Usage = usage
	flag.Parse()

	if *relay == "" || *name == "" {
		usage()
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	opts := []transfers.Option{transfers.WithPort(*port)}
	if *insecure {
		opts = append(opts, transfers.Insecure())
	}
	if !*noProgress {
		prog := transfers.NewProgress(os.Stderr)
		defer prog.Stop()
		opts = append(opts, transfers.WithProgress(prog))
	}

	switch args[0] {

	case "serve":
		// serve [-n N]
		n := 1
		for i := 1; i+1 < len(args); i++ {
			if args[i] == "-n" {
				if v, err := strconv.Atoi(args[i+1]); err == nil && v > 0 {
					n = v
				}
			}
		}
		log.Printf("serving as %q (slots=%d)", *name, n)
		if err := transfers.ServeLoop(*relay, *name, n, opts...); err != nil {
			log.Fatalf("serve: %v", err)
		}

	case "copy":
		if len(args) != 3 {
			log.Fatal("usage: copy <src> <dst>")
		}
		if err := transfers.Copy(*relay, *name, args[1], args[2], opts...); err != nil {
			log.Fatalf("copy: %v", err)
		}

	case "copy-many":
		rest := args[1:]
		if len(rest) == 0 || len(rest)%2 != 0 {
			log.Fatal("copy-many requires an even number of arguments: src0 dst0 src1 dst1 ...")
		}
		pairs := make([][2]string, len(rest)/2)
		for i := range pairs {
			pairs[i] = [2]string{rest[i*2], rest[i*2+1]}
		}
		log.Printf("copying %d item(s) in parallel as %q", len(pairs), *name)
		if err := transfers.CopyMany(*relay, *name, pairs, opts...); err != nil {
			log.Fatalf("copy-many: %v", err)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n\n", args[0])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: client -relay HOST -name NAME [flags] <command> [args]

flags:
  -relay HOST      relay hostname (required)
  -name  NAME      well-known name for this machine (required)
  -port  PORT      relay port (default 443)
  -insecure        skip TLS certificate verification
  -no-progress     disable the terminal progress bar

commands:
  serve [-n N]
      Register and serve N incoming transfers (default 1).
      Use -n to accept multiple transfers in parallel.

  copy <src> <dst>
      Copy a file or directory tree.
      Exactly one side must be "peer:/path"; the other is a local path.
      Interrupted transfers are resumed automatically from the last chunk.

  copy-many <src0> <dst0> [<src1> <dst1> ...]
      Copy multiple items in parallel (one connection per pair).
      The remote side must run "serve -n N" with N >= number of pairs.

spec format:
  machineA:/data/archive.tar   remote peer named "machineA"
  ./local/dir/                 local path (file or directory)
  me:/local/path               local path (explicit self-reference)

examples:
  # Machine A — serve 4 parallel slots:
  client -relay relay.example.com -name machineA serve -n 4

  # Machine B — pull a whole directory:
  client -relay relay.example.com -name machineB copy machineA:/data/ ./data/

  # Machine B — push 4 files in parallel:
  client -relay relay.example.com -name machineB copy-many \
      ./file0.tar machineA:/dest/file0.tar \
      ./file1.tar machineA:/dest/file1.tar \
      ./file2.tar machineA:/dest/file2.tar \
      ./file3.tar machineA:/dest/file3.tar
`)
}
