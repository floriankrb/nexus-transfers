package transfers

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Progress tracks transfer progress and renders a live terminal status line.
// It is safe for concurrent use from multiple goroutines.
type Progress struct {
	totalFiles int
	totalBytes int64

	doneFiles atomic.Int64
	doneBytes atomic.Int64

	mu          sync.Mutex
	currentFile string
	currentSize int64

	startTime time.Time
	output    io.Writer

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewProgress creates a Progress that writes to w (typically os.Stderr).
// Call Stop() when the transfer is complete to flush the final line.
func NewProgress(w io.Writer) *Progress {
	p := &Progress{
		output:    w,
		startTime: time.Now(),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	go p.loop()
	return p
}

// SetManifest initialises totals once the manifest is known.
func (p *Progress) SetManifest(totalFiles int, totalBytes int64) {
	p.totalFiles = totalFiles
	p.totalBytes = totalBytes
}

// StartFile records that a file is now being transferred.
// offset is the resume offset (already-transferred bytes, pre-counted).
func (p *Progress) StartFile(path string, size, offset int64) {
	p.mu.Lock()
	p.currentFile = path
	p.currentSize = size
	p.mu.Unlock()
}

// AddBytes is called with each chunk's byte count as it completes.
func (p *Progress) AddBytes(n int64) {
	p.doneBytes.Add(n)
}

// FinishFile marks the current file as done.
func (p *Progress) FinishFile() {
	p.doneFiles.Add(1)
}

// Stop finalises the progress display and waits for the render goroutine to exit.
func (p *Progress) Stop() {
	close(p.stopCh)
	<-p.doneCh
	fmt.Fprintln(p.output) // leave cursor on a new line
}

// ── render loop ───────────────────────────────────────────────────────────────

func (p *Progress) loop() {
	defer close(p.doneCh)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			p.render()
		case <-p.stopCh:
			p.render() // one final render
			return
		}
	}
}

const barWidth = 20

func (p *Progress) render() {
	done := p.doneBytes.Load()
	total := p.totalBytes
	doneF := p.doneFiles.Load()
	totalF := int64(p.totalFiles)

	elapsed := time.Since(p.startTime).Seconds()
	var speedMBs float64
	if elapsed > 0 {
		speedMBs = float64(done) / elapsed / 1e6
	}

	var eta string
	if speedMBs > 0 && total > done {
		remaining := float64(total-done) / 1e6 / speedMBs
		eta = "ETA " + formatDuration(time.Duration(remaining*float64(time.Second)))
	} else if done >= total && total > 0 {
		eta = "done"
	} else {
		eta = "ETA ?"
	}

	pct := 0.0
	if total > 0 {
		pct = math.Min(float64(done)/float64(total), 1.0)
	}

	bar := renderBar(pct, barWidth)

	p.mu.Lock()
	cur := p.currentFile
	p.mu.Unlock()
	if len(cur) > 35 {
		cur = "…" + cur[len(cur)-34:]
	}

	line := fmt.Sprintf("\r%s %3.0f%% | %d/%d files | %s/%s | %.1f MB/s | %s | %s",
		bar,
		pct*100,
		doneF, totalF,
		formatBytes(done), formatBytes(total),
		speedMBs,
		eta,
		cur,
	)
	fmt.Fprint(p.output, line)
}

func renderBar(pct float64, width int) string {
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
