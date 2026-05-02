package transfers

// Wire protocol used between two clients through the relay pipe.
//
// After the relay handshake (GO / SEND <path> / RECV <path>) both sides
// communicate with a simple framed binary protocol. The relay is a dumb pipe
// at this point.
//
// Frame layout (9-byte header + payload):
//
//   [1]  message type
//   [4]  payload length, big-endian uint32
//   [4]  CRC-32/IEEE of payload, big-endian uint32
//   [N]  payload bytes
//
// Message flow (sender → receiver, except RESUME which is receiver → sender):
//
//   sender  → MANIFEST  (file list with sizes)
//   receiver→ RESUME    (which files / offsets to skip)
//   for each file:
//     sender → FILE_START
//     sender → CHUNK  (repeated, up to chunkSize each)
//     sender → FILE_DONE (sha-256 of whole file)
//   sender  → DONE
//   receiver→ DONE      (acknowledgement)

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	msgManifest  byte = 0x01
	msgResume    byte = 0x02
	msgFileStart byte = 0x03
	msgChunk     byte = 0x04
	msgFileDone  byte = 0x05
	msgDone      byte = 0x06
	msgError     byte = 0x07
)

// chunkSize is the maximum payload carried in a single CHUNK frame.
// 4 MiB balances throughput vs. memory and gives fine-grained resume
// boundaries without flooding the frame log.
const chunkSize = 4 << 20

// ── frame I/O ────────────────────────────────────────────────────────────────

func writeFrame(w io.Writer, typ byte, payload []byte) error {
	crc := crc32.ChecksumIEEE(payload)
	hdr := [9]byte{}
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[5:9], crc)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) (typ byte, payload []byte, err error) {
	var hdr [9]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	typ = hdr[0]
	payLen := binary.BigEndian.Uint32(hdr[1:5])
	wantCRC := binary.BigEndian.Uint32(hdr[5:9])

	payload = make([]byte, payLen)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}
	if got := crc32.ChecksumIEEE(payload); got != wantCRC {
		err = fmt.Errorf("CRC mismatch (got %08x want %08x) on frame type %02x", got, wantCRC, typ)
	}
	return
}

func writeJSON(w io.Writer, typ byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFrame(w, typ, b)
}

func readExpectJSON(r io.Reader, wantType byte, v any) error {
	typ, payload, err := readFrame(r)
	if err != nil {
		return err
	}
	if typ == msgError {
		return fmt.Errorf("remote error: %s", payload)
	}
	if typ != wantType {
		return fmt.Errorf("unexpected frame type %02x (want %02x)", typ, wantType)
	}
	return json.Unmarshal(payload, v)
}

func writeError(w io.Writer, msg string) {
	writeFrame(w, msgError, []byte(msg)) //nolint:errcheck
}

// ── protocol structs ─────────────────────────────────────────────────────────

// FileEntry describes one file in the transfer manifest.
type FileEntry struct {
	Index int64  `json:"i"`
	Path  string `json:"p"` // relative to transfer root
	Size  int64  `json:"s"`
}

// Manifest is the first message sent by the sender.
type Manifest struct {
	Files      []FileEntry `json:"files"`
	TotalBytes int64       `json:"total_bytes"`
}

// ResumeEntry tells the sender to start a file at a given byte offset.
type ResumeEntry struct {
	Index  int64 `json:"i"`
	Offset int64 `json:"o"`
}

// Resume is sent by the receiver after it has inspected its local state.
type Resume struct {
	Files []ResumeEntry `json:"files"`
}

// FileStart is sent immediately before the CHUNK stream for one file.
type FileStart struct {
	Index  int64  `json:"i"`
	Path   string `json:"p"`
	Size   int64  `json:"s"`
	Offset int64  `json:"o"` // byte offset at which CHUNK stream begins
}

// FileDone is sent after all chunks for a file have been written.
// SHA256 covers the whole file (0..Size), regardless of resume offset.
type FileDone struct {
	Index  int64  `json:"i"`
	SHA256 string `json:"sha256"` // hex
}
