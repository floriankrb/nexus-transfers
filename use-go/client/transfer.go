package transfers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// stateDir is created inside the destination root to persist resume state.
const stateDir = ".transfers"

// ── sender ───────────────────────────────────────────────────────────────────

// RunSender walks srcRoot (file or directory), sends the manifest, waits for
// the receiver's resume reply, then streams all files over w/r.
func RunSender(r io.Reader, w io.Writer, srcRoot string, prog *Progress) error {
	entries, err := buildManifest(srcRoot)
	if err != nil {
		writeError(w, err.Error())
		return err
	}

	var totalBytes int64
	for _, e := range entries {
		totalBytes += e.Size
	}
	manifest := Manifest{Files: entries, TotalBytes: totalBytes}

	if err := writeJSON(w, msgManifest, manifest); err != nil {
		return fmt.Errorf("send manifest: %w", err)
	}

	// Read resume state from receiver.
	var resume Resume
	if err := readExpectJSON(r, msgResume, &resume); err != nil {
		return fmt.Errorf("read resume: %w", err)
	}
	offsets := make(map[int64]int64, len(resume.Files))
	for _, re := range resume.Files {
		offsets[re.Index] = re.Offset
	}

	if prog != nil {
		prog.SetManifest(len(entries), totalBytes)
		for _, re := range resume.Files {
			if int(re.Index) < len(entries) {
				prog.AddBytes(re.Offset)
			}
		}
	}

	for _, entry := range entries {
		offset := offsets[entry.Index]
		absPath := filepath.Join(srcRoot, entry.Path)
		if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
			// single-file transfer: srcRoot IS the file
			absPath = srcRoot
		}

		if prog != nil {
			prog.StartFile(entry.Path, entry.Size, offset)
		}

		sha256hex, err := sendFile(r, w, absPath, entry, offset, prog)
		if err != nil {
			writeError(w, fmt.Sprintf("file %s: %v", entry.Path, err))
			return err
		}

		if err := writeJSON(w, msgFileDone, FileDone{Index: entry.Index, SHA256: sha256hex}); err != nil {
			return err
		}
		if prog != nil {
			prog.FinishFile()
		}
	}

	if err := writeFrame(w, msgDone, nil); err != nil {
		return err
	}
	// Wait for receiver's DONE ack.
	typ, _, err := readFrame(r)
	if err != nil {
		return err
	}
	if typ != msgDone {
		return fmt.Errorf("expected DONE ack, got frame %02x", typ)
	}
	return nil
}

func buildManifest(root string) ([]FileEntry, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}

	var entries []FileEntry
	idx := int64(0)

	if !info.IsDir() {
		entries = append(entries, FileEntry{
			Index: 0,
			Path:  filepath.Base(root),
			Size:  info.Size(),
		})
		return entries, nil
	}

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		entries = append(entries, FileEntry{Index: idx, Path: rel, Size: fi.Size()})
		idx++
		return nil
	})
	return entries, err
}

func sendFile(r io.Reader, w io.Writer, absPath string, entry FileEntry, offset int64, prog *Progress) (string, error) {
	hdr := FileStart{Index: entry.Index, Path: entry.Path, Size: entry.Size, Offset: offset}
	if err := writeJSON(w, msgFileStart, hdr); err != nil {
		return "", err
	}

	f, err := os.Open(absPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()

	// Hash the already-transferred portion without sending it.
	if offset > 0 {
		if _, err := io.CopyN(h, f, offset); err != nil {
			return "", fmt.Errorf("seeking past resume offset: %w", err)
		}
	}

	buf := make([]byte, chunkSize)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			if err := writeFrame(w, msgChunk, chunk); err != nil {
				return "", err
			}
			if prog != nil {
				prog.AddBytes(int64(n))
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// ── receiver ─────────────────────────────────────────────────────────────────

// RunReceiver reads the manifest from r, sends the resume reply, then receives
// all files into dstRoot, verifying SHA-256 on completion of each file.
func RunReceiver(r io.Reader, w io.Writer, dstRoot string, prog *Progress) error {
	var manifest Manifest
	if err := readExpectJSON(r, msgManifest, &manifest); err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(dstRoot, stateDir), 0o755); err != nil {
		return err
	}

	// Build resume reply from persisted state.
	resume := buildResume(dstRoot, manifest.Files)
	if err := writeJSON(w, msgResume, resume); err != nil {
		return fmt.Errorf("send resume: %w", err)
	}

	if prog != nil {
		prog.SetManifest(len(manifest.Files), manifest.TotalBytes)
		for _, re := range resume.Files {
			prog.AddBytes(re.Offset)
		}
	}

	// Receive files until DONE.
	for {
		typ, payload, err := readFrame(r)
		if err != nil {
			return err
		}
		switch typ {
		case msgFileStart:
			var fs FileStart
			if err := json.Unmarshal(payload, &fs); err != nil {
				return err
			}
			if prog != nil {
				prog.StartFile(fs.Path, fs.Size, fs.Offset)
			}
			if err := receiveFile(r, w, dstRoot, fs, prog); err != nil {
				return err
			}
			if prog != nil {
				prog.FinishFile()
			}

		case msgDone:
			// Send ack, then we're done.
			writeFrame(w, msgDone, nil) //nolint:errcheck
			return nil

		case msgError:
			return fmt.Errorf("sender error: %s", payload)

		default:
			return fmt.Errorf("unexpected frame type %02x", typ)
		}
	}
}

func receiveFile(r io.Reader, w io.Writer, dstRoot string, fs FileStart, prog *Progress) error {
	rel := filepath.FromSlash(fs.Path)
	dstPath := filepath.Join(dstRoot, rel)
	tmpPath := dstPath + ".tmp"
	statePath := stateFilePath(dstRoot, fs.Index)

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}

	// Open temp file: append if resuming, create otherwise.
	flags := os.O_CREATE | os.O_WRONLY
	if fs.Offset > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(tmpPath, flags, 0o644)
	if err != nil {
		return err
	}

	h := sha256.New()
	written := fs.Offset

	// If resuming, seed the hasher with what we already have.
	if fs.Offset > 0 {
		existing, err := os.Open(tmpPath)
		if err != nil {
			// Can't verify existing data; restart from 0.
			f.Close()
			existing.Close()
			return receiveFileFromScratch(r, w, dstRoot, fs, prog)
		}
		if _, err := io.CopyN(h, existing, fs.Offset); err != nil {
			existing.Close()
			f.Close()
			return receiveFileFromScratch(r, w, dstRoot, fs, prog)
		}
		existing.Close()
	}

	// Persist state so we can resume if interrupted.
	saveState(statePath, stateRecord{
		Path:     fs.Path,
		Size:     fs.Size,
		Received: written,
		Done:     false,
	})

	// Drain CHUNK frames.
	for {
		typ, payload, err := readFrame(r)
		if err != nil {
			f.Close()
			return err
		}
		switch typ {
		case msgChunk:
			if _, err := f.Write(payload); err != nil {
				f.Close()
				return err
			}
			h.Write(payload)
			written += int64(len(payload))
			if prog != nil {
				prog.AddBytes(int64(len(payload)))
			}
			// Update state every 64 MiB so crash leaves a useful checkpoint.
			if written%(64<<20) == 0 {
				saveState(statePath, stateRecord{
					Path: fs.Path, Size: fs.Size,
					Received: written, Done: false,
				})
			}

		case msgFileDone:
			f.Close()
			var fd FileDone
			if err := json.Unmarshal(payload, &fd); err != nil {
				return err
			}
			gotSHA := hex.EncodeToString(h.Sum(nil))
			if gotSHA != fd.SHA256 {
				// Corruption: delete tmp and state, signal caller to retry.
				os.Remove(tmpPath)
				os.Remove(statePath)
				return fmt.Errorf("SHA-256 mismatch for %s (got %s want %s)", fs.Path, gotSHA[:8], fd.SHA256[:8])
			}
			// Atomically replace the destination file.
			if err := os.Rename(tmpPath, dstPath); err != nil {
				return err
			}
			saveState(statePath, stateRecord{
				Path: fs.Path, Size: fs.Size,
				Received: fs.Size, Done: true, SHA256: fd.SHA256,
			})
			return nil

		case msgError:
			f.Close()
			return fmt.Errorf("sender error: %s", payload)

		default:
			f.Close()
			return fmt.Errorf("unexpected frame %02x while receiving chunks", typ)
		}
	}
}

func receiveFileFromScratch(r io.Reader, w io.Writer, dstRoot string, fs FileStart, prog *Progress) error {
	// Drain the chunk stream (we can't skip it) — caller should re-request from 0.
	// For simplicity: return an error; the user can re-run and the sender will
	// restart that file from 0 because the state file won't be present.
	return fmt.Errorf("cannot seed hasher for %s; delete partial file and retry", fs.Path)
}

// ── resume state ─────────────────────────────────────────────────────────────

type stateRecord struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Received int64  `json:"received"`
	Done     bool   `json:"done"`
	SHA256   string `json:"sha256,omitempty"`
}

func stateFilePath(dstRoot string, index int64) string {
	return filepath.Join(dstRoot, stateDir, fmt.Sprintf("%d.state", index))
}

func saveState(path string, s stateRecord) {
	b, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(path, b, 0o644) //nolint:errcheck
}

func loadState(path string) (stateRecord, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return stateRecord{}, false
	}
	var s stateRecord
	if err := json.Unmarshal(b, &s); err != nil {
		return stateRecord{}, false
	}
	return s, true
}

func buildResume(dstRoot string, files []FileEntry) Resume {
	var resume Resume
	for _, fe := range files {
		sp := stateFilePath(dstRoot, fe.Index)
		s, ok := loadState(sp)
		if !ok {
			continue
		}
		if s.Done {
			// Verify the final file still exists and matches size.
			dstPath := filepath.Join(dstRoot, filepath.FromSlash(fe.Path))
			if info, err := os.Stat(dstPath); err == nil && info.Size() == fe.Size {
				// Fully done — tell sender to skip entirely (offset == size).
				resume.Files = append(resume.Files, ResumeEntry{Index: fe.Index, Offset: fe.Size})
				continue
			}
			// File missing or wrong size; restart.
			os.Remove(sp)
			continue
		}
		// Partial transfer: validate the temp file.
		tmpPath := filepath.Join(dstRoot, filepath.FromSlash(fe.Path)) + ".tmp"
		info, err := os.Stat(tmpPath)
		if err != nil {
			continue
		}
		// Align offset to the last full chunk boundary.
		confirmed := alignToChunk(info.Size())
		if confirmed <= 0 {
			continue
		}
		// Truncate the temp file to the confirmed boundary to discard any
		// incomplete final chunk that may have been partially written.
		if info.Size() > confirmed {
			if err := truncateFile(tmpPath, confirmed); err != nil {
				os.Remove(sp)
				continue
			}
		}
		resume.Files = append(resume.Files, ResumeEntry{Index: fe.Index, Offset: confirmed})
	}
	return resume
}

func alignToChunk(size int64) int64 {
	if size <= 0 {
		return 0
	}
	n := size / chunkSize
	return n * chunkSize
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}

// cleanStateForSkippedFiles removes stale state entries for paths that have
// been fully transferred (offset == size) so they don't accumulate.
func cleanState(dstRoot string, files []FileEntry) {
	for _, fe := range files {
		s, ok := loadState(stateFilePath(dstRoot, fe.Index))
		if ok && s.Done {
			dstPath := filepath.Join(dstRoot, filepath.FromSlash(fe.Path))
			// If the real file now exists and is correct, we can remove the state.
			if info, err := os.Stat(dstPath); err == nil && info.Size() == fe.Size {
				os.Remove(stateFilePath(dstRoot, fe.Index))
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// isDir reports whether path is an existing directory.
func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// sanitizePath prevents path traversal attacks in received relative paths.
func sanitizePath(rel string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("path escapes root: %s", rel)
	}
	return clean, nil
}
