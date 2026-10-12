package report

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/beesar/net-peer-benchmark/internal/bench"
)

// LiveOptions controls the progress renderer.
type LiveOptions struct {
	// TTY redraws one line in place with \r. Otherwise the renderer prints
	// plain lines: phase start, phase end, and one line every PlainEvery.
	TTY bool
	// Interval between redraws; default 250ms.
	Interval time.Duration
	// PlainEvery spaces periodic lines when TTY is false; default 3s.
	PlainEvery time.Duration
}

const (
	maxLineWidth = 79
	rateWindow   = time.Second
)

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Live samples a bench.Progress from its own goroutine and writes progress.
// It never blocks the measurement: it only reads atomics via Snapshot.
type Live struct {
	w    io.Writer
	p    *bench.Progress
	opt  LiveOptions
	stop chan struct{}
	done chan struct{}
	once sync.Once

	// Owned by the render goroutine.
	lastWidth int
	frame     int
	upRate    rateTracker
	downRate  rateTracker
	started   map[bench.Phase]bool
	ended     map[bench.Phase]bool
	lastPlain time.Time
}

// Rates are the recent TCP rates in bytes/s, one per direction.
type Rates struct {
	Upload   float64
	Download float64
}

type rateSample struct {
	at    time.Time
	bytes uint64
}

type rateTracker struct {
	samples []rateSample
}

// StartLive begins rendering p to w until Stop is called.
func StartLive(w io.Writer, p *bench.Progress, opt LiveOptions) *Live {
	if opt.Interval <= 0 {
		opt.Interval = 250 * time.Millisecond
	}
	if opt.PlainEvery <= 0 {
		opt.PlainEvery = 3 * time.Second
	}
	l := &Live{
		w:       w,
		p:       p,
		opt:     opt,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		started: make(map[bench.Phase]bool),
		ended:   make(map[bench.Phase]bool),
	}
	go l.run()
	return l
}

// Stop ends rendering. On a TTY the progress line is cleared; otherwise any
// pending phase start/end lines are flushed. Safe on nil and repeat calls.
func (l *Live) Stop() {
	if l == nil {
		return
	}
	l.once.Do(func() { close(l.stop) })
	<-l.done
}

func (l *Live) run() {
	defer close(l.done)
	t := time.NewTicker(l.opt.Interval)
	defer t.Stop()
	l.tick(time.Now())
	for {
		select {
		case <-l.stop:
			if l.opt.TTY {
				l.clear()
			} else {
				l.announce(l.p.Snapshot(), time.Now())
			}
			return
		case now := <-t.C:
			l.tick(now)
		}
	}
}

func (l *Live) tick(now time.Time) {
	s := l.p.Snapshot()
	rate := Rates{
		Upload:   l.upRate.update(s.Upload, now),
		Download: l.downRate.update(s.Download, now),
	}
	if l.opt.TTY {
		l.draw(spinner[l.frame%len(spinner)] + " " + ProgressLine(s, rate))
		l.frame++
		return
	}
	l.announce(s, now)
	if running(s) && now.Sub(l.lastPlain) >= l.opt.PlainEvery {
		fmt.Fprintf(l.w, "npbench: %s\n", ProgressLine(s, rate))
		l.lastPlain = now
	}
}

// announce prints start/end lines in phase order, so a phase shorter than one
// tick still gets both.
func (l *Live) announce(s bench.ProgressSnapshot, now time.Time) {
	var phases []bench.Phase
	switch {
	case s.Stream.Started || s.Phase == bench.PhaseStream:
		phases = []bench.Phase{bench.PhaseStream}
	case s.MTU.Started || s.Phase == bench.PhaseMTU:
		phases = []bench.Phase{bench.PhaseMTU}
	case s.Bidir:
		phases = []bench.Phase{bench.PhaseLatency, bench.PhaseBidir}
	default:
		phases = []bench.Phase{bench.PhaseLatency, bench.PhaseUpload, bench.PhaseDownload}
	}
	for _, ph := range phases {
		started, finished := phaseState(s, ph)
		if started && !l.started[ph] {
			l.started[ph] = true
			l.lastPlain = now
			fmt.Fprintf(l.w, "npbench: %s\n", PhaseStartLine(s, ph))
		}
		if finished && !l.ended[ph] {
			l.ended[ph] = true
			fmt.Fprintf(l.w, "npbench: %s\n", PhaseEndLine(s, ph))
		}
	}
}

func phaseState(s bench.ProgressSnapshot, ph bench.Phase) (started, finished bool) {
	switch ph {
	case bench.PhaseLatency:
		return s.Latency.Started, s.Latency.Finished
	case bench.PhaseUpload:
		return s.Upload.Started, s.Upload.Finished
	case bench.PhaseDownload:
		return s.Download.Started, s.Download.Finished
	case bench.PhaseBidir:
		return s.Upload.Started || s.Download.Started, s.Upload.Finished && s.Download.Finished
	case bench.PhaseStream:
		return s.Stream.Started, s.Stream.Finished
	case bench.PhaseMTU:
		return s.MTU.Started, s.MTU.Finished
	}
	return false, false
}

func running(s bench.ProgressSnapshot) bool {
	started, finished := phaseState(s, s.Phase)
	return started && !finished
}

// update returns bytes/s of t over roughly the last second.
func (r *rateTracker) update(t bench.TCPProgress, now time.Time) float64 {
	if !t.Started {
		r.samples = r.samples[:0]
		return 0
	}
	if len(r.samples) == 0 {
		r.samples = append(r.samples, rateSample{at: now.Add(-t.Elapsed)})
	}
	r.samples = append(r.samples, rateSample{at: now, bytes: t.Bytes})
	for len(r.samples) > 2 && now.Sub(r.samples[1].at) >= rateWindow {
		r.samples = r.samples[1:]
	}
	first := r.samples[0]
	dt := now.Sub(first.at).Seconds()
	if dt <= 0 || t.Bytes < first.bytes {
		return 0
	}
	return float64(t.Bytes-first.bytes) / dt
}

func (l *Live) draw(line string) {
	line = truncate(line, maxLineWidth)
	w := utf8.RuneCountInString(line)
	pad := ""
	if l.lastWidth > w {
		pad = strings.Repeat(" ", l.lastWidth-w)
	}
	fmt.Fprintf(l.w, "\r%s%s", line, pad)
	l.lastWidth = w
}

func (l *Live) clear() {
	if l.lastWidth == 0 {
		return
	}
	fmt.Fprintf(l.w, "\r%s\r", strings.Repeat(" ", l.lastWidth))
	l.lastWidth = 0
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}

// ProgressLine is the live status of the current phase, without a spinner
// or line ending. r holds the recent TCP rates (see Live).
func ProgressLine(s bench.ProgressSnapshot, r Rates) string {
	switch s.Phase {
	case bench.PhaseIdle:
		return "starting"
	case bench.PhaseLatency:
		l := s.Latency
		line := fmt.Sprintf("latency   sent %d/%d  recv %d  loss %.1f%%", l.Sent, l.Total, l.Recv, l.LossPct())
		if l.Recv > 0 {
			line += fmt.Sprintf("  rtt %.2f ms", ms(l.LastRTT))
		}
		if l.HasJitter {
			line += fmt.Sprintf("  jitter %.2f ms", l.JitterMs)
		}
		return line
	case bench.PhaseUpload:
		return tcpLine(s.Phase, s.Upload, r.Upload)
	case bench.PhaseDownload:
		return tcpLine(s.Phase, s.Download, r.Download)
	case bench.PhaseBidir:
		// Rates drop to 5 digits so both directions fit in 80 columns.
		up, down := s.Upload, s.Download
		return fmt.Sprintf("⇅ bidir  %s/%s  up now %s avg %s  down now %s avg %s Mbps",
			seconds(max(up.Elapsed, down.Elapsed)), seconds(max(up.Planned, down.Planned)),
			shortMbps(r.Upload), shortMbps(avgBps(up)),
			shortMbps(r.Download), shortMbps(avgBps(down)))
	case bench.PhaseStream:
		st := s.Stream
		line := fmt.Sprintf("stream   %s/%s  sent %d  recv %d  loss %.1f%%",
			seconds(st.Elapsed), seconds(st.Planned), st.Sent, st.Recv, st.LossPct())
		if st.HasJitter {
			line += fmt.Sprintf("  jitter %.1f ms", st.JitterMs)
		}
		line += fmt.Sprintf("  %.2f Mbps", st.AchievedBps()/1e6)
		return line
	case bench.PhaseMTU:
		m := s.MTU
		return fmt.Sprintf("mtu      trying %d  ok %d  hi %d", m.Trying, m.OK, m.Hi)
	}
	return "finishing"
}

func tcpLine(ph bench.Phase, t bench.TCPProgress, rateBps float64) string {
	return fmt.Sprintf("%-8s  %s/%s  %s  now %.1f Mbps  avg %.1f Mbps",
		ph, seconds(t.Elapsed), seconds(t.Planned), HumanBytes(t.Bytes),
		bench.Mbps(rateBps), bench.Mbps(avgBps(t)))
}

// shortMbps keeps one decimal below 1000 Mbps and none above.
func shortMbps(bps float64) string {
	m := bench.Mbps(bps)
	if m < 999.95 {
		return fmt.Sprintf("%.1f", m)
	}
	return fmt.Sprintf("%.0f", m)
}

// PhaseStartLine is the plain-mode line printed when ph begins.
func PhaseStartLine(s bench.ProgressSnapshot, ph bench.Phase) string {
	switch ph {
	case bench.PhaseLatency:
		return fmt.Sprintf("latency start (%d UDP probes)", s.Latency.Total)
	case bench.PhaseUpload:
		return tcpStartLine("upload", s.Upload)
	case bench.PhaseDownload:
		return tcpStartLine("download", s.Download)
	case bench.PhaseBidir:
		return fmt.Sprintf("bidir start (%s, %s per direction, simultaneous)",
			seconds(s.Upload.Planned), streamCount(s.Upload.Streams))
	case bench.PhaseStream:
		st := s.Stream
		return fmt.Sprintf("stream start (%s, %s CBR, %d-byte packets)",
			seconds(st.Planned), bench.FormatBitrate(st.Bitrate), st.Packet)
	case bench.PhaseMTU:
		return fmt.Sprintf("mtu start (probe %d..%d)", s.MTU.Lo, s.MTU.Hi)
	}
	return ph.String()
}

func tcpStartLine(name string, t bench.TCPProgress) string {
	return fmt.Sprintf("%s start (%s, %s)", name, seconds(t.Planned), streamCount(t.Streams))
}

func streamCount(n int) string {
	if n == 1 {
		return "1 stream"
	}
	return fmt.Sprintf("%d streams", n)
}

// PhaseEndLine is the plain-mode line printed when ph finishes.
func PhaseEndLine(s bench.ProgressSnapshot, ph bench.Phase) string {
	switch ph {
	case bench.PhaseLatency:
		l := s.Latency
		line := fmt.Sprintf("latency done  sent %d  recv %d  loss %.1f%%", l.Sent, l.Recv, l.LossPct())
		if l.HasJitter {
			line += fmt.Sprintf("  jitter %.2f ms", l.JitterMs)
		}
		return line
	case bench.PhaseUpload:
		return tcpEndLine("upload", s.Upload)
	case bench.PhaseDownload:
		return tcpEndLine("download", s.Download)
	case bench.PhaseBidir:
		up, down := s.Upload, s.Download
		return fmt.Sprintf("bidir done  %s  up %s avg %.1f Mbps  down %s avg %.1f Mbps",
			seconds(max(up.Elapsed, down.Elapsed)),
			HumanBytes(up.Bytes), bench.Mbps(avgBps(up)),
			HumanBytes(down.Bytes), bench.Mbps(avgBps(down)))
	case bench.PhaseStream:
		st := s.Stream
		line := fmt.Sprintf("stream done  %s  sent %d  recv %d  loss %.1f%%",
			seconds(st.Elapsed), st.Sent, st.Recv, st.LossPct())
		if st.HasJitter {
			line += fmt.Sprintf("  jitter %.2f ms", st.JitterMs)
		}
		line += fmt.Sprintf("  %.2f Mbps", st.AchievedBps()/1e6)
		return line
	case bench.PhaseMTU:
		m := s.MTU
		return fmt.Sprintf("mtu done  udp payload %d bytes", m.OK)
	}
	return ph.String()
}

func tcpEndLine(name string, t bench.TCPProgress) string {
	return fmt.Sprintf("%s done  %s  %s  avg %.1f Mbps",
		name, seconds(t.Elapsed), HumanBytes(t.Bytes), bench.Mbps(avgBps(t)))
}

func avgBps(t bench.TCPProgress) float64 {
	if t.Elapsed <= 0 {
		return 0
	}
	return float64(t.Bytes) / t.Elapsed.Seconds()
}
